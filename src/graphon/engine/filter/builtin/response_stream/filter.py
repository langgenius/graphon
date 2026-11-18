from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, cast

from graphon.engine.filter.protocol import EngineEventFilterContext
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
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.file.runtime import use_workflow_file_runtime
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol

from . import snapshot, v4
from .frame_graph import build_child_graph, find_response_container_ids
from .frame_session import _FrameSession
from .reply_order import _ReplyOrder
from .snapshot import ResponseStreamFilterSnapshot, SnapshotHeader

__all__ = ["ResponseStreamFilter"]


class ResponseStreamFilter:
    """Select and assemble responses outside execution, with independent frame state."""

    def __init__(self, *, pass_unmatched_chunks: bool = False) -> None:
        self._pass_unmatched_chunks = pass_unmatched_chunks
        self._file_runtime: WorkflowFileRuntimeProtocol | None = None
        self._pending_state: SnapshotHeader | None = None
        self._root_session: _FrameSession | None = None
        self._frame_sessions: dict[str, _FrameSession] = {}
        self._graph_config: Mapping[str, Any] | None = None
        self._awaiting_root_baseline = False
        self._context: EngineEventFilterContext | None = None
        self._run_id: str | None = None
        self._run_started = False
        self._suppress_child_responses = False
        self._finished_frame_ids: set[str] = set()
        self._reply_order = _ReplyOrder()
        self._response_container_ids: set[str] = set()

    def initialize(self, context: EngineEventFilterContext) -> None:
        if (
            self._root_session is not None
            and self._run_started
            and self._pending_state is None
            and context.run_id is not None
            and context.run_id == self._run_id
        ):
            self._context = context
            self._file_runtime = context.file_runtime
            self._root_session.graph = cast(GraphProtocol, context.graph)
            return
        self._restore_progress(context, self._pending_state)

    def _restore_progress(
        self, context: EngineEventFilterContext, state: SnapshotHeader | None
    ) -> None:
        # Validate separately before replacing live progress.
        with use_workflow_file_runtime(context.file_runtime):
            graph_config = getattr(context.graph, "graph_config", None)
            container_ids = find_response_container_ids(graph_config)
            if isinstance(state, v4.Snapshot):
                sessions, reply_order = v4.restore(
                    state, context, pass_unmatched_chunks=self._pass_unmatched_chunks
                )
                awaiting_baseline = state.awaiting_root_baseline
                suppress_child_responses = state.suppress_children
                run_started = state.started
                finished_frame_ids = set(state.finished_frames)
            else:
                reply_order = _ReplyOrder()
                awaiting_baseline = context.run_id is not None
                root = _FrameSession(
                    cast(GraphProtocol, context.graph),
                    None if awaiting_baseline else context.runtime_state.variable_pool,
                    reply_order=reply_order,
                    pass_unmatched_chunks=self._pass_unmatched_chunks,
                )
                if state is not None:
                    root.load_state(cast(ResponseStreamFilterSnapshot, state))
                else:
                    root.register_responses()
                sessions = {ROOT_FRAME_ID: root}
                # Legacy snapshots cannot establish child display history or order.
                suppress_child_responses = state is not None
                run_started = state is not None
                finished_frame_ids = set()
            self._context = context
            self._run_id = context.run_id
            self._file_runtime = context.file_runtime
            self._graph_config = graph_config
            self._response_container_ids = container_ids
            self._frame_sessions = sessions
            self._root_session = sessions[ROOT_FRAME_ID]
            self._reply_order = reply_order
            self._awaiting_root_baseline = awaiting_baseline
            self._suppress_child_responses = suppress_child_responses
            self._run_started = run_started
            self._finished_frame_ids = finished_frame_ids
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

    def _process_event(self, event: EngineEvent) -> list[EngineEvent]:  # ruff: ignore[too-many-return-statements, complex-structure]
        match event:
            case FrameStartedEvent():
                return self._start_frame_session(event)
            case GraphRunStartedEvent():
                if (
                    event.reason == WorkflowStartReason.RESUMPTION
                    and self._run_id is not None
                    and not self._run_started
                ):
                    msg = "Resuming a run requires saved ResponseStreamFilter progress"
                    raise ValueError(msg)
                self._run_started = True
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
                    if session is not None:
                        self._finished_frame_ids.add(event.frame_id)
                    self._frame_sessions.pop(event.frame_id, None)
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

    def _update_container_replies(self, event: EngineEvent) -> None:
        if self._suppress_child_responses or not isinstance(event, NodeEvent):
            return
        if event.node_id not in self._response_container_ids:
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
            output = self._initialized_root_session.start(event)
            self._awaiting_root_baseline = False
            self._run_started = True
            return output
        if session := self._frame_sessions.get(event.frame_id):
            return session.start(event)
        if (
            event.frame_id in self._finished_frame_ids
            or self._suppress_child_responses
            or event.container_id not in self._response_container_ids
        ):
            return []
        if event.reason == WorkflowStartReason.RESUMPTION:
            msg = f"Missing child response progress for frame {event.frame_id}"
            raise ValueError(msg)
        self._run_started = True
        self._reply_order.start_frame(event)
        graph = build_child_graph(self._graph_config, event)
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
        """Save progress after draining the filtered run through its pause boundary."""
        if self._pending_state is not None:
            return snapshot.dumps(self._pending_state)
        self._ensure_initialized()
        return snapshot.dumps(v4.capture(self))

    def loads(self, data: str) -> None:
        """Stage or atomically restore progress; rebind host adapters at initialize."""
        state = snapshot.loads(data)
        if self._context is None or self._pending_state is not None:
            self._pending_state = state
            return
        self._restore_progress(self._context, state)

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
