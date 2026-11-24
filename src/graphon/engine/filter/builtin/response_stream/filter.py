from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, cast

from graphon.engine.filter.protocol import EngineEventFilterContext
from graphon.engine.frame_graph import FrameGraph
from graphon.engine.frame_graph_factory import (
    FrameGraphFactory,
    build_local_graph_config,
)
from graphon.engine_events.base import EngineEvent, NodeEvent
from graphon.engine_events.frame import (
    FrameFinishedEvent,
    FrameStartedEvent,
    FrameVariablesUpdatedEvent,
)
from graphon.engine_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunReasoningChunkEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.engine_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import BuiltinNodeTypes
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.file.runtime import use_workflow_file_runtime
from graphon.graph import Graph, NodeDefinition
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol

from . import snapshot
from .frame_graph import build_child_graph, find_response_container_ids
from .frame_session import _FrameSession
from .reply_order import _ReplyOrder
from .snapshot import ResponseStreamFilterSnapshot

__all__ = ["ResponseStreamFilter"]


class ResponseStreamFilter:
    """Select and assemble responses outside execution, with independent frame state."""

    def __init__(self, *, pass_unmatched_chunks: bool = False) -> None:
        self._pass_unmatched_chunks = pass_unmatched_chunks
        self._file_runtime: WorkflowFileRuntimeProtocol | None = None
        self._pending_state: ResponseStreamFilterSnapshot | None = None
        self._root_session: _FrameSession | None = None
        self._frame_sessions: dict[str, _FrameSession] = {}
        self._graph_config: Mapping[str, Any] | None = None
        self._awaiting_root_baseline = False
        self._has_child_progress = False
        self._reply_order = _ReplyOrder()
        self._response_container_ids: set[str] = set()
        self._frame_graph_factory: FrameGraphFactory | None = None
        self._response_containers_by_frame: dict[str, dict[str, NodeDefinition]] = {}

    def initialize(self, context: EngineEventFilterContext) -> None:
        with use_workflow_file_runtime(context.file_runtime):
            self._file_runtime = context.file_runtime
            self._root_session = None
            self._frame_sessions.clear()
            self._reply_order.clear()
            self._has_child_progress = False
            self._awaiting_root_baseline = context.run_id is not None
            root_session = _FrameSession(
                cast(GraphProtocol, context.graph),
                None
                if self._awaiting_root_baseline
                else context.runtime_state.variable_pool,
                reply_order=self._reply_order,
                pass_unmatched_chunks=self._pass_unmatched_chunks,
            )
            if self._pending_state is not None:
                root_session.load_state(self._pending_state)
            else:
                root_session.register_responses()
            self._root_session = root_session
            self._frame_sessions[ROOT_FRAME_ID] = root_session
            self._graph_config = getattr(context.graph, "graph_config", None)
            self._response_containers_by_frame.clear()
            self._frame_graph_factory = (
                context.graph.frame_graph_factory
                if isinstance(context.graph, FrameGraph)
                else None
            )
            if (
                self._frame_graph_factory is not None
                and context.graph.definition is not None
            ):
                self._register_response_containers(
                    ROOT_FRAME_ID, context.graph.definition
                )
            else:
                self._response_container_ids = find_response_container_ids(
                    self._graph_config
                )
            self._pending_state = None

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]:
        self._ensure_initialized()
        with use_workflow_file_runtime(self._file_runtime):
            output = self._process_event(event)
            chunks = self._reply_order.drain()
            if isinstance(
                event,
                (
                    GraphRunSucceededEvent,
                    GraphRunPartialSucceededEvent,
                    GraphRunFailedEvent,
                    GraphRunAbortedEvent,
                ),
            ):
                self._reply_order.clear()
            if isinstance(event, GraphRunStartedEvent):
                return [*output, *chunks]
            return [*chunks, *output]

    def _process_event(self, event: EngineEvent) -> list[EngineEvent]:  # ruff: ignore[too-many-return-statements]
        match event:
            case FrameStartedEvent():
                return self._start_frame_session(event)
            case GraphRunStartedEvent():
                return [
                    event,
                    *(
                        []
                        if self._awaiting_root_baseline
                        else self._initialized_root_session.activate_initial_sessions()
                    ),
                ]
            case FrameFinishedEvent():
                session = self._frame_sessions.get(event.frame_id)
                output = (
                    session.finish(succeeded=event.outcome == "succeeded")
                    if session
                    else []
                )
                if event.frame_id != ROOT_FRAME_ID:
                    self._frame_sessions.pop(event.frame_id, None)
                    self._response_containers_by_frame.pop(event.frame_id, None)
                self._reply_order.finish_frame(
                    event.frame_id, succeeded=event.outcome == "succeeded"
                )
                return [*output, event]
            case (
                GraphRunSucceededEvent()
                | GraphRunPartialSucceededEvent()
                | GraphRunFailedEvent()
                | GraphRunAbortedEvent()
            ):
                succeeded = isinstance(
                    event, (GraphRunSucceededEvent, GraphRunPartialSucceededEvent)
                )
                output = [
                    chunk
                    for session in self._frame_sessions.values()
                    for chunk in session.finish(succeeded=succeeded)
                ]
                self._frame_sessions.clear()
                self._reply_order.finish(succeeded=succeeded)
                return [*output, event]
            case (
                FrameVariablesUpdatedEvent()
                | NodeEvent()
                | GraphEdgeTakenEvent()
                | GraphEdgeSkippedEvent()
            ):
                if session := self._frame_sessions.get(event.frame_id):
                    self._update_container_replies(event)
                    return session.on_event(event)
                if isinstance(
                    event,
                    (
                        FrameVariablesUpdatedEvent,
                        GraphEdgeTakenEvent,
                        GraphEdgeSkippedEvent,
                        NodeRunReasoningChunkEvent,
                    ),
                ):
                    return []
                if (
                    isinstance(event, NodeRunStreamChunkEvent)
                    and not self._pass_unmatched_chunks
                ):
                    return []
        return [event]

    def _register_response_containers(self, frame_id: str, graph: Graph) -> None:
        frame_graph_factory = self._frame_graph_factory
        if frame_graph_factory is None:
            return
        callers: dict[str, NodeDefinition] = {}
        for caller in graph.nodes.values():
            if caller.call_target is None:
                continue
            body = frame_graph_factory.resolve_call_target(caller)
            if any(
                node.node_type in {BuiltinNodeTypes.ANSWER, BuiltinNodeTypes.END}
                for scope, _ in frame_graph_factory.walk_graphs(body)
                for node in scope.nodes.values()
            ):
                callers[caller.id] = caller
        self._response_containers_by_frame[frame_id] = callers

    def _container_has_response(self, frame_id: str, node_id: str) -> bool:
        if self._frame_graph_factory is not None:
            return node_id in self._response_containers_by_frame.get(frame_id, {})
        return node_id in self._response_container_ids

    def _update_container_replies(self, event: EngineEvent) -> None:
        if not isinstance(event, NodeEvent):
            return
        if not self._container_has_response(event.frame_id, event.node_id):
            return
        if isinstance(
            event,
            (
                NodeRunSucceededEvent,
                NodeRunExceptionEvent,
                NodeRunFailedEvent,
                NodeRunRetryEvent,
            ),
        ):
            self._reply_order.finish_container(event)
        if isinstance(event, NodeRunStartedEvent):
            self._reply_order.start_container(event)

    def _start_frame_session(self, event: FrameStartedEvent) -> list[EngineEvent]:
        # Full-value observations are consumed here, never forwarded to the host.
        if event.frame_id == ROOT_FRAME_ID:
            self._awaiting_root_baseline = False
            return self._initialized_root_session.start(event)
        parent_frame_id = event.parent_frame_id or ROOT_FRAME_ID
        if not self._container_has_response(parent_frame_id, event.container_id):
            return []
        if event.reason == WorkflowStartReason.RESUMPTION:
            msg = "Resuming child response progress is not supported yet"
            raise ValueError(msg)
        self._reply_order.start_frame(event)
        config = self._graph_config
        configured_start_node_id = None
        if self._frame_graph_factory is not None:
            caller = self._response_containers_by_frame[parent_frame_id][
                event.container_id
            ]
            body = self._frame_graph_factory.resolve_call_target(caller)
            configured_start_node_id = caller.config.get("start_node_id")
            self._register_response_containers(event.frame_id, body)
            config = build_local_graph_config(body, event.container_id)
        graph = build_child_graph(
            config, event, configured_start_node_id=configured_start_node_id
        )
        if graph is None:
            return []
        session = _FrameSession(
            graph,
            None,
            reply_order=self._reply_order,
            frame_id=event.frame_id,
            container_id=event.container_id,
            pass_unmatched_chunks=self._pass_unmatched_chunks,
        )
        session.register_responses()
        if session.has_responses:
            self._has_child_progress = True
        self._frame_sessions[event.frame_id] = session
        return session.start(event)

    def flush(self) -> Iterable[EngineEvent]:
        self._ensure_initialized()
        with use_workflow_file_runtime(self._file_runtime):
            output = [
                event
                for session in self._frame_sessions.values()
                for event in session.flush()
            ]
            return [*output, *self._reply_order.drain()]

    def dumps(self) -> str:
        self._check_snapshot_scope()
        if self._pending_state is not None:
            return snapshot.dumps(self._pending_state)
        self._ensure_initialized()
        return snapshot.dumps(self._initialized_root_session.dump_state())

    def loads(self, data: str) -> None:
        self._check_snapshot_scope()
        with use_workflow_file_runtime(self._file_runtime):
            state = snapshot.loads(data)
            if self._root_session is None or self._pending_state is not None:
                self._pending_state = state
                return
            self._root_session.load_state(state)

    def _check_snapshot_scope(self) -> None:
        # Root checkpoints do not yet include child response progress.
        if self._has_child_progress or self._reply_order.has_pending_containers:
            msg = "Snapshots containing child response progress are not supported yet"
            raise ValueError(msg)

    def _ensure_initialized(self) -> None:
        if self._root_session is None or self._pending_state is not None:
            msg = "ResponseStreamFilter must be initialized before use."
            raise RuntimeError(msg)

    @property
    def _initialized_root_session(self) -> _FrameSession:
        if self._root_session is None:
            msg = "ResponseStreamFilter must be initialized before use."
            raise RuntimeError(msg)
        return self._root_session
