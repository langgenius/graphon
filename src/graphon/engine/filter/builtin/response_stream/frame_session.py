"""Root and child response sessions owned by ResponseStreamFilter.

Each frame follows its own traversal and drains templates into the filter output.
It retains observed values independently of the engine's live frame registry.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import cast

from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import FrameStartedEvent, FrameVariablesUpdatedEvent
from graphon.engine_events.node import (
    NodeRunExceptionEvent,
    NodeRunReasoningChunkEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.engine_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.nodes.base.template import VariableSegment
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol
from graphon.runtime.runtime_state_protocol import ReadOnlyVariablePool
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import Variable

from . import snapshot, v2
from .snapshot import ResponseSessionState, ResponseStreamFilterSnapshot, Selector
from .template_stream import _ResponseSession, _StreamBuffers, _TemplateStream

type _NodeID = str
type _EdgeID = str
type _Selector = Selector


@dataclass
class _BlockingPath:
    """Blocking traversal edges that must be taken before a response can stream."""

    edge_ids: list[_EdgeID] = field(default_factory=list)

    def remove_edge(self, edge_id: _EdgeID) -> None:
        if edge_id in self.edge_ids:
            self.edge_ids.remove(edge_id)

    def is_empty(self) -> bool:
        return len(self.edge_ids) == 0


class _FrameSession:
    """Traversal, captured values, and response cursors for one execution frame."""

    def __init__(
        self,
        graph: GraphProtocol,
        variable_pool: ReadOnlyVariablePool | None,
        *,
        frame_id: str = ROOT_FRAME_ID,
        container_id: str = "",
        pass_unmatched_chunks: bool = False,
    ) -> None:
        self.graph = graph
        self.variable_pool = variable_pool
        self.frame_id = frame_id
        self._pass_unmatched_chunks = pass_unmatched_chunks
        self._observed_values: VariablePool | None = None
        self._skipped_edge_ids: set[str] | None = None
        self._active_session: _ResponseSession | None = None
        self._waiting_sessions: deque[_ResponseSession] = deque()
        self._template_stream = _TemplateStream(
            frame_id=frame_id, container_id=container_id
        )
        self._response_node_ids: set[str] = set()
        self._blocking_paths: dict[str, list[_BlockingPath]] = {}
        self._response_sessions: dict[str, _ResponseSession] = {}
        self._referenced_selectors: set[_Selector] = set()

    @property
    def has_responses(self) -> bool:
        return bool(self._response_node_ids)

    def register_responses(self) -> None:
        for node in self.graph.nodes.values():
            if node.execution_type == NodeExecutionType.RESPONSE and (
                self.frame_id == ROOT_FRAME_ID
                or node.node_type in {BuiltinNodeTypes.ANSWER, BuiltinNodeTypes.END}
            ):
                self._register_response(node.id)

    def start(self, event: FrameStartedEvent) -> list[EngineEvent]:
        self._observed_values = VariablePool()
        self.variable_pool = self._observed_values
        if event.reason == WorkflowStartReason.INITIAL:
            self._skipped_edge_ids = set()
        self._template_stream.skipped_node_ids = self._find_skipped_node_ids()
        self._template_stream.node_execution_ids.update(event.node_execution_ids)
        self._replace_values(event.variables)
        return [*self.activate_initial_sessions(), *self._try_flush()]

    def _replace_values(self, variables: Iterable[Variable]) -> None:
        if self._observed_values is not None:
            for variable in variables:
                self._observed_values.add(
                    variable.selector, variable.model_copy(deep=True)
                )
                self._template_stream.forget_replaced_streams(variable.selector)

    def on_event(self, event: EngineEvent) -> list[EngineEvent]:  # ruff: ignore[too-many-return-statements]
        match event:
            case FrameVariablesUpdatedEvent():
                self._replace_values(event.variables)
                return self._try_flush()
            case NodeRunRetryEvent():
                self._template_stream.close_previous_attempts(event)
                self._template_stream.node_execution_ids[event.node_id] = event.id
                return [*self._try_flush(), event]
            case NodeRunStartedEvent():
                self._template_stream.node_execution_ids[event.node_id] = event.id
            case NodeRunStreamChunkEvent():
                return self._handle_stream_chunk(event)
            case NodeRunReasoningChunkEvent():
                return self._handle_reasoning_chunk(event)
            case GraphEdgeTakenEvent():
                return self._handle_edge_taken(event.edge_id)
            case GraphEdgeSkippedEvent():
                return self._handle_edge_skipped(event)
            case NodeRunSucceededEvent() | NodeRunExceptionEvent():
                self._template_stream.finish_source(event)
                return [*self._try_flush(), event]
        return [event]

    def flush(self) -> list[EngineEvent]:
        return self._try_flush()

    def finish(self, *, succeeded: bool) -> list[EngineEvent]:
        events: list[EngineEvent] = []
        if succeeded:
            self._template_stream.finish_sources(self.graph.nodes)
            events.extend(self._try_flush())
        if self._active_session is not None and self._active_session.emitted:
            events.extend(
                self._template_stream.finish_response(
                    self._active_session, graph=self.graph
                )
            )
        self._active_session = None
        self._waiting_sessions.clear()
        self._response_sessions.clear()
        self._referenced_selectors.clear()
        self.variable_pool = None
        self._observed_values = None
        self._template_stream.clear()
        return events

    def _find_skipped_node_ids(self) -> set[str] | None:
        if self._skipped_edge_ids is None:
            return None
        reachable: set[str] = set()
        pending = [self.graph.root_node.id]
        while pending:
            node_id = pending.pop()
            if node_id in reachable:
                continue
            reachable.add(node_id)
            pending.extend(
                edge.head
                for edge in self.graph.get_outgoing_edges(node_id)
                if edge.id not in self._skipped_edge_ids
            )
        return set(self.graph.nodes) - reachable

    def _handle_edge_skipped(self, event: GraphEdgeSkippedEvent) -> list[EngineEvent]:
        if self._skipped_edge_ids is not None:
            self._skipped_edge_ids.add(event.edge_id)
            self._template_stream.skipped_node_ids = self._find_skipped_node_ids()
        for node_id, paths in self._blocking_paths.items():
            self._blocking_paths[node_id] = [
                path for path in paths if event.edge_id not in path.edge_ids
            ]
        return self._try_flush()

    def dump_state(self) -> v2.Snapshot:
        return v2.Snapshot(
            skipped_edges=sorted(self._skipped_edge_ids)
            if self._skipped_edge_ids is not None
            else None,
            response_nodes=sorted(self._response_node_ids),
            active_session=self._serialize_session(self._active_session),
            waiting_sessions=[
                session_state
                for session in list(self._waiting_sessions)
                if (session_state := self._serialize_session(session)) is not None
            ],
            pending_sessions=[
                session_state
                for _, session in sorted(self._response_sessions.items())
                if (session_state := self._serialize_session(session)) is not None
            ],
            node_execution_ids=dict(
                sorted(self._template_stream.node_execution_ids.items())
            ),
            paths_map={
                node_id: [path.edge_ids.copy() for path in paths]
                for node_id, paths in sorted(self._blocking_paths.items())
            },
            stream_buffers=self._template_stream.stream_buffers.dump_buffers(),
            stream_positions=self._template_stream.stream_buffers.dump_positions(),
            closed_streams=self._template_stream.stream_buffers.dump_closed_selectors(),
        )

    def load_state(self, state: ResponseStreamFilterSnapshot) -> None:
        state = cast(v2.Snapshot, snapshot.for_graph(state, self.graph))

        response_node_ids = set(state.response_nodes)
        blocking_paths = {
            node_id: [_BlockingPath(edge_ids=list(path_edges)) for path_edges in paths]
            for node_id, paths in state.paths_map.items()
        }
        node_execution_ids = dict(state.node_execution_ids)

        stream_buffers = _StreamBuffers.from_state(
            buffers=state.stream_buffers,
            positions=state.stream_positions,
            closed_selectors=state.closed_streams,
        )
        waiting_sessions = deque(
            self._session_from_state(session_state)
            for session_state in state.waiting_sessions
        )
        response_sessions = {
            session_state.node_id: self._session_from_state(session_state)
            for session_state in state.pending_sessions
        }
        active_session = (
            self._session_from_state(state.active_session)
            if state.active_session
            else None
        )

        referenced_selectors: set[_Selector] = set()
        for response_node_id in response_node_ids:
            referenced_selectors.update(
                self._get_referenced_selectors(response_node_id)
            )

        self._skipped_edge_ids = (
            set(state.skipped_edges) if state.skipped_edges is not None else None
        )
        self._active_session = active_session
        self._waiting_sessions = waiting_sessions
        self._template_stream = _TemplateStream(
            stream_buffers=stream_buffers,
            node_execution_ids=node_execution_ids,
            skipped_node_ids=self._find_skipped_node_ids(),
        )
        self._response_node_ids = response_node_ids
        self._blocking_paths = blocking_paths
        self._response_sessions = response_sessions
        self._referenced_selectors = referenced_selectors

    def _register_response(self, response_node_id: _NodeID) -> None:
        if response_node_id in self._response_node_ids:
            return
        self._response_node_ids.add(response_node_id)
        self._blocking_paths[response_node_id] = self._build_blocking_paths(
            response_node_id
        )

        response_node = self.graph.nodes[response_node_id]
        self._response_sessions[response_node_id] = _ResponseSession.from_node(
            response_node,
        )
        self._record_referenced_selectors(response_node_id)

    def _record_referenced_selectors(self, response_node_id: _NodeID) -> None:
        self._referenced_selectors.update(
            self._get_referenced_selectors(response_node_id)
        )

    def _get_referenced_selectors(
        self,
        response_node_id: _NodeID,
    ) -> set[_Selector]:
        response_node = self.graph.nodes.get(response_node_id)
        if response_node is None:
            return set()

        response_session = _ResponseSession.from_node(response_node)
        return {
            tuple(segment.selector)
            for segment in response_session.template.segments
            if isinstance(segment, VariableSegment)
        }

    def _build_blocking_paths(self, response_node_id: _NodeID) -> list[_BlockingPath]:
        root_node_id = self.graph.root_node.id
        if root_node_id == response_node_id:
            return [_BlockingPath()]

        variable_selectors = self._get_response_variable_selectors(response_node_id)
        all_complete_paths = self._find_all_paths(root_node_id, response_node_id)
        return [
            _BlockingPath(edge_ids=self._get_blocking_edges(path, variable_selectors))
            for path in all_complete_paths
        ]

    def _get_response_variable_selectors(
        self,
        response_node_id: _NodeID,
    ) -> set[_Selector]:
        response_node = self.graph.nodes[response_node_id]
        response_session = _ResponseSession.from_node(response_node)
        return {
            tuple(segment.selector[:2])
            for segment in response_session.template.segments
            if isinstance(segment, VariableSegment)
        }

    def _find_all_paths(
        self,
        current_node_id: _NodeID,
        target_node_id: _NodeID,
        current_path: list[_EdgeID] | None = None,
        visited: set[_NodeID] | None = None,
    ) -> list[list[_EdgeID]]:
        current_path = current_path or []
        visited = visited or set()
        if current_node_id == target_node_id:
            return [current_path.copy()]

        next_visited = {current_node_id, *visited}
        paths: list[list[_EdgeID]] = []
        for edge in self.graph.get_outgoing_edges(current_node_id):
            if edge.head in next_visited:
                continue
            paths.extend(
                self._find_all_paths(
                    edge.head,
                    target_node_id,
                    [*current_path, edge.id],
                    next_visited,
                ),
            )
        return paths

    def _get_blocking_edges(
        self,
        path: list[_EdgeID],
        variable_selectors: set[_Selector],
    ) -> list[_EdgeID]:
        return [
            edge_id
            for edge_id in path
            if self._is_blocking_edge(edge_id, variable_selectors)
        ]

    def _is_blocking_edge(
        self,
        edge_id: _EdgeID,
        variable_selectors: set[_Selector],
    ) -> bool:
        edge = self.graph.edges[edge_id]
        source_node = self.graph.nodes[edge.tail]
        return source_node.execution_type in frozenset((
            NodeExecutionType.BRANCH,
            NodeExecutionType.CONTAINER,
            NodeExecutionType.RESPONSE,
        )) or source_node.blocks_variable_output(variable_selectors)

    def activate_initial_sessions(self) -> list[EngineEvent]:
        events: list[EngineEvent] = []
        for response_node_id in sorted(self._response_node_ids):
            paths = self._blocking_paths.get(response_node_id, [])
            if any(path.is_empty() for path in paths):
                events.extend(self._activate_or_queue_session(response_node_id))
        return events

    def _handle_edge_taken(self, edge_id: _EdgeID) -> list[EngineEvent]:
        events: list[EngineEvent] = []
        for response_node_id in sorted(self._response_node_ids):
            paths = self._blocking_paths.get(response_node_id)
            if paths is None:
                continue

            has_reachable_path = False
            for path in paths:
                path.remove_edge(edge_id)
                if path.is_empty():
                    has_reachable_path = True

            if has_reachable_path:
                events.extend(self._activate_or_queue_session(response_node_id))
        return events

    def _activate_or_queue_session(
        self,
        node_id: _NodeID,
    ) -> list[EngineEvent]:
        session = self._response_sessions.pop(node_id, None)
        if session is None:
            return []

        if self._active_session is None:
            self._active_session = session
            return self._try_flush()

        self._waiting_sessions.append(session)
        return []

    def _handle_stream_chunk(
        self,
        event: NodeRunStreamChunkEvent,
    ) -> list[EngineEvent]:
        selector_key = tuple(event.selector)
        if selector_key in self._referenced_selectors:
            self._template_stream.append(event)
            return self._try_flush()
        if self._pass_unmatched_chunks:
            return [event]
        return []

    def _handle_reasoning_chunk(
        self,
        event: NodeRunReasoningChunkEvent,
    ) -> list[EngineEvent]:
        if self._is_reasoning_visible(event):
            return [event]
        return []

    def _is_reasoning_visible(self, event: NodeRunReasoningChunkEvent) -> bool:
        return (
            self._has_valid_reasoning_selector(event)
            and self._has_runnable_reasoning_source(event.node_id)
            and self._is_reasoning_referenced_by_reached_session(event)
        )

    @staticmethod
    def _has_valid_reasoning_selector(event: NodeRunReasoningChunkEvent) -> bool:
        return tuple(event.selector) == (event.node_id, "reasoning_content")

    def _has_runnable_reasoning_source(self, node_id: _NodeID) -> bool:
        source_node = self.graph.nodes.get(node_id)
        return bool(
            source_node
            and not self._template_stream.is_skipped(node_id, graph=self.graph)
        )

    def _is_reasoning_referenced_by_reached_session(
        self,
        event: NodeRunReasoningChunkEvent,
    ) -> bool:
        reasoning_key = tuple(event.selector)
        companion_text_key = (event.node_id, "text")
        return any(
            self._session_references_reasoning_source(
                session,
                selector_prefixes={reasoning_key, companion_text_key},
            )
            for session in self._reached_sessions()
        )

    def _reached_sessions(self) -> tuple[_ResponseSession, ...]:
        sessions: list[_ResponseSession] = []
        if self._active_session is not None:
            sessions.append(self._active_session)
        sessions.extend(self._waiting_sessions)
        return tuple(sessions)

    @staticmethod
    def _session_references_reasoning_source(
        session: _ResponseSession,
        selector_prefixes: set[_Selector],
    ) -> bool:
        return any(
            isinstance(segment, VariableSegment)
            and tuple(segment.selector[:2]) in selector_prefixes
            for segment in session.template.segments
        )

    def _try_flush(self) -> list[EngineEvent]:
        if not self._active_session or self.variable_pool is None:
            return []

        response_node_id = self._active_session.node_id
        events: list[EngineEvent] = []
        events.extend(
            self._template_stream.flush(
                self._active_session,
                graph=self.graph,
                variable_pool=self.variable_pool,
            )
        )
        if events:
            self._active_session.emitted = True
        if self._active_session.is_complete():
            events.extend(self._end_session(response_node_id))

        return events

    def _end_session(self, node_id: _NodeID) -> list[EngineEvent]:
        if not self._active_session or self._active_session.node_id != node_id:
            return []

        events: list[EngineEvent] = list(
            self._template_stream.finish_response(
                self._active_session, graph=self.graph
            )
        )
        self._active_session = None
        if self._waiting_sessions:
            self._active_session = self._waiting_sessions.popleft()
            events.extend(self._try_flush())
        return events

    def _serialize_session(
        self,
        session: _ResponseSession | None,
    ) -> ResponseSessionState | None:
        if session is None:
            return None
        return ResponseSessionState(node_id=session.node_id, index=session.index)

    def _session_from_state(
        self,
        session_state: ResponseSessionState,
    ) -> _ResponseSession:
        node = self.graph.nodes.get(session_state.node_id)
        if node is None:
            msg = f"Unknown response node '{session_state.node_id}' in serialized state"
            raise ValueError(msg)

        session = _ResponseSession.from_node(node)
        session.index = session_state.index
        return session
