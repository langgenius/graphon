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
    NodeRunReasoningChunkEvent,
    NodeRunStreamChunkEvent,
)
from graphon.engine_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.file.runtime import use_workflow_file_runtime
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol

from . import snapshot
from .frame_graph import build_child_graph
from .frame_session import _FrameSession
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

    def initialize(self, context: EngineEventFilterContext) -> None:
        with use_workflow_file_runtime(context.file_runtime):
            self._file_runtime = context.file_runtime
            self._root_session = None
            self._frame_sessions.clear()
            self._has_child_progress = False
            self._awaiting_root_baseline = context.run_id is not None
            root_session = _FrameSession(
                cast(GraphProtocol, context.graph),
                None
                if self._awaiting_root_baseline
                else context.runtime_state.variable_pool,
                pass_unmatched_chunks=self._pass_unmatched_chunks,
            )
            if self._pending_state is not None:
                root_session.load_state(self._pending_state)
            else:
                root_session.register_responses()
            self._root_session = root_session
            self._frame_sessions[ROOT_FRAME_ID] = root_session
            self._graph_config = getattr(context.graph, "graph_config", None)
            self._pending_state = None

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]:
        self._ensure_initialized()
        with use_workflow_file_runtime(self._file_runtime):
            return self._process_event(event)

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
                return [*output, event]
            case (
                FrameVariablesUpdatedEvent()
                | NodeEvent()
                | GraphEdgeTakenEvent()
                | GraphEdgeSkippedEvent()
            ):
                if session := self._frame_sessions.get(event.frame_id):
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

    def _start_frame_session(self, event: FrameStartedEvent) -> list[EngineEvent]:
        # Full-value observations are consumed here, never forwarded to the host.
        if event.frame_id == ROOT_FRAME_ID:
            self._awaiting_root_baseline = False
            return self._initialized_root_session.start(event)
        graph = build_child_graph(self._graph_config, event)
        if graph is None:
            return []
        session = _FrameSession(
            graph,
            None,
            frame_id=event.frame_id,
            container_id=event.container_id,
            pass_unmatched_chunks=self._pass_unmatched_chunks,
        )
        session.register_responses()
        if session.has_responses:
            if event.reason == WorkflowStartReason.RESUMPTION:
                msg = "Resuming child response progress is not supported yet"
                raise ValueError(msg)
            self._has_child_progress = True
        self._frame_sessions[event.frame_id] = session
        return session.start(event)

    def flush(self) -> Iterable[EngineEvent]:
        self._ensure_initialized()
        with use_workflow_file_runtime(self._file_runtime):
            return [
                event
                for session in self._frame_sessions.values()
                for event in session.flush()
            ]

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
        # shortcut: v1/v2 store root progress only; add child snapshots in stage F.
        if self._has_child_progress:
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
