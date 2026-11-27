"""Root and child response sessions owned by ResponseStreamFilter.

Each frame follows its own traversal and drains templates into the filter output.
It retains observed values independently of the engine's live frame registry.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

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
from graphon.enums import BuiltinNodeTypes, FailBranchSourceHandle, NodeExecutionType
from graphon.nodes.base.template import VariableSegment
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol
from graphon.runtime.runtime_state_protocol import ReadOnlyVariablePool
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import Variable

from . import snapshot, v3
from .ids import EdgeID, NodeID
from .reachability import _EdgeDecision, _ResponseReachability
from .reply_order import _Reply, _ReplyOrder
from .snapshot import ResponseSessionState, ResponseStreamFilterSnapshot, Selector
from .template_stream import _ResponseSession, _StreamBuffers, _TemplateStream

type _Selector = Selector


@dataclass
class _BlockingPath:
    """Blocking traversal edges that must be taken before a response can stream."""

    edge_ids: list[EdgeID] = field(default_factory=list)

    def remove_edge(self, edge_id: EdgeID) -> None:
        if edge_id in self.edge_ids:
            self.edge_ids.remove(edge_id)

    def is_empty(self) -> bool:
        return len(self.edge_ids) == 0


@dataclass
class _ReachabilityState:
    """Frame-owned first decisions and response-local reachability caches."""

    first_edge_decisions: dict[EdgeID, _EdgeDecision] = field(default_factory=dict)
    reachability_by_response: dict[NodeID, _ResponseReachability] = field(
        default_factory=dict
    )


@dataclass
class _LegacyPathState:
    """Residual conditions retained from an imported legacy checkpoint."""

    paths_by_response: dict[NodeID, list[_BlockingPath]]


class _FrameSession:
    """Traversal, captured values, and response cursors for one execution frame."""

    def __init__(
        self,
        graph: GraphProtocol,
        variable_pool: ReadOnlyVariablePool | None,
        *,
        reply_order: _ReplyOrder,
        frame_id: str = ROOT_FRAME_ID,
        container_id: str = "",
        pass_unmatched_chunks: bool = False,
    ) -> None:
        self.graph = graph
        self.variable_pool = variable_pool
        self.frame_id = frame_id
        self._reply_order = reply_order
        self._replies: dict[str, _Reply] = {}
        self._completed_sessions: dict[str, _ResponseSession] = {}
        self._pass_unmatched_chunks = pass_unmatched_chunks
        self._observed_values: VariablePool | None = None
        self._skipped_edge_ids: set[EdgeID] | None = None
        self._active_session: _ResponseSession | None = None
        self._waiting_sessions: deque[_ResponseSession] = deque()
        self._template_stream = _TemplateStream(
            frame_id=frame_id, container_id=container_id
        )
        self._response_node_ids: set[NodeID] = set()
        self._gating_state: _ReachabilityState | _LegacyPathState = _ReachabilityState()
        self._response_sessions: dict[NodeID, _ResponseSession] = {}
        self._referenced_selectors: set[_Selector] = set()

    @property
    def has_responses(self) -> bool:
        return bool(self._response_node_ids)

    def register_responses(self) -> None:
        gating_state = self._gating_state
        if not isinstance(gating_state, _ReachabilityState):
            msg = "Cannot register fresh responses in legacy path state"
            raise TypeError(msg)
        for node in self.graph.nodes.values():
            if node.execution_type == NodeExecutionType.RESPONSE and (
                self.frame_id == ROOT_FRAME_ID
                or node.node_type in {BuiltinNodeTypes.ANSWER, BuiltinNodeTypes.END}
            ):
                self._register_response(NodeID(node.id), gating_state)

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
        self._release_displayed_sessions()
        match event:
            case FrameVariablesUpdatedEvent():
                self._replace_values(event.variables)
                return self._try_flush()
            case NodeRunRetryEvent():
                self._template_stream.close_previous_attempts(event)
                self._restart_hidden_replies(event)
                self._template_stream.node_execution_ids[event.node_id] = event.id
                return [*self._try_flush(), event]
            case NodeRunStartedEvent():
                self._template_stream.node_execution_ids[event.node_id] = event.id
            case NodeRunStreamChunkEvent():
                return self._handle_stream_chunk(event)
            case NodeRunReasoningChunkEvent():
                return self._handle_reasoning_chunk(event)
            case GraphEdgeTakenEvent():
                return self._handle_edge_taken(EdgeID(event.edge_id))
            case GraphEdgeSkippedEvent():
                return self._handle_edge_skipped(event)
            case NodeRunExceptionEvent():
                abandoned = self._abandon_sessions_for_failed_node(
                    NodeID(event.node_id)
                )
                self._template_stream.finish_source(event)
                return [*abandoned, *self._try_flush(), event]
            case NodeRunSucceededEvent():
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
            self._replies[self._active_session.node_id].events.extend(
                self._template_stream.finish_response(
                    self._active_session, graph=self.graph
                )
            )
        for reply in self._replies.values():
            reply.finished = True
        self._replies.clear()
        self._completed_sessions.clear()
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
                if EdgeID(edge.id) not in self._skipped_edge_ids
            )
        return set(self.graph.nodes) - reachable

    def _handle_edge_skipped(self, event: GraphEdgeSkippedEvent) -> list[EngineEvent]:
        edge_id = EdgeID(event.edge_id)
        if self._skipped_edge_ids is not None:
            self._skipped_edge_ids.add(edge_id)
            self._template_stream.skipped_node_ids = self._find_skipped_node_ids()
        gating_state = self._gating_state
        if isinstance(gating_state, _LegacyPathState):
            for node_id, paths in gating_state.paths_by_response.items():
                gating_state.paths_by_response[node_id] = [
                    path for path in paths if edge_id not in path.edge_ids
                ]
        elif edge_id in self.graph.edges:
            gating_state.first_edge_decisions.setdefault(edge_id, _EdgeDecision.SKIPPED)
        return self._try_flush()

    def dump_state(self) -> v3.Snapshot:
        gating_state = self._gating_state
        if isinstance(gating_state, _ReachabilityState):
            taken_edges = sorted(
                edge_id
                for edge_id, decision in gating_state.first_edge_decisions.items()
                if decision is _EdgeDecision.TAKEN
            )
            rejected_edges = sorted(
                edge_id
                for edge_id, decision in gating_state.first_edge_decisions.items()
                if decision is _EdgeDecision.SKIPPED
            )
            legacy_paths_map = None
        else:
            taken_edges = []
            rejected_edges = []
            legacy_paths_map = {
                node_id: [path.edge_ids.copy() for path in paths]
                for node_id, paths in sorted(gating_state.paths_by_response.items())
            }
        return v3.Snapshot(
            skipped_edges=sorted(self._skipped_edge_ids)
            if self._skipped_edge_ids is not None
            else None,
            response_nodes=[
                str(node_id) for node_id in sorted(self._response_node_ids)
            ],
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
            taken_edges=taken_edges,
            rejected_edges=rejected_edges,
            legacy_paths_map=legacy_paths_map,
            stream_buffers=self._template_stream.stream_buffers.dump_buffers(),
            stream_positions=self._template_stream.stream_buffers.dump_positions(),
            closed_streams=self._template_stream.stream_buffers.dump_closed_selectors(),
        )

    def load_state(self, state: ResponseStreamFilterSnapshot) -> None:
        state = snapshot.for_graph(state, self.graph)
        if not isinstance(state, v3.Snapshot):
            msg = "Expected migrated version 3 ResponseStreamFilter state"
            raise TypeError(msg)

        response_node_ids = {NodeID(node_id) for node_id in state.response_nodes}
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
            NodeID(session_state.node_id): self._session_from_state(session_state)
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

        gating_state: _ReachabilityState | _LegacyPathState
        if state.legacy_paths_map is not None:
            gating_state = _LegacyPathState({
                node_id: [
                    _BlockingPath(edge_ids=list(path_edges)) for path_edges in paths
                ]
                for node_id, paths in state.legacy_paths_map.items()
            })
        else:
            first_edge_decisions = dict.fromkeys(state.taken_edges, _EdgeDecision.TAKEN)
            # Earlier v3 writers also recorded takes after a first skip. Preserve
            # that input: rejection wins when an edge occurs in both wire lists.
            first_edge_decisions.update(
                dict.fromkeys(state.rejected_edges, _EdgeDecision.SKIPPED)
            )
            gating_state = _ReachabilityState(
                first_edge_decisions=first_edge_decisions,
                reachability_by_response={
                    node_id: self._build_reachability(node_id, first_edge_decisions)
                    for node_id in response_node_ids
                },
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
        self._gating_state = gating_state
        self._response_sessions = response_sessions
        self._referenced_selectors = referenced_selectors
        self._reply_order.clear()
        self._completed_sessions.clear()
        self._replies = {
            session.node_id: self._reply_order.add_reply(self.frame_id)
            for session in self._reached_sessions()
        }

    def _register_response(
        self, response_node_id: NodeID, gating_state: _ReachabilityState
    ) -> None:
        if response_node_id in self._response_node_ids:
            return
        self._response_node_ids.add(response_node_id)
        gating_state.reachability_by_response[response_node_id] = (
            self._build_reachability(
                response_node_id, gating_state.first_edge_decisions
            )
        )

        response_node = self.graph.nodes[response_node_id]
        self._response_sessions[response_node_id] = _ResponseSession.from_node(
            response_node,
        )
        self._record_referenced_selectors(response_node_id)

    def _record_referenced_selectors(self, response_node_id: NodeID) -> None:
        self._referenced_selectors.update(
            self._get_referenced_selectors(response_node_id)
        )

    def _get_referenced_selectors(
        self,
        response_node_id: NodeID,
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

    def _build_reachability(
        self,
        response_node_id: NodeID,
        first_edge_decisions: Mapping[EdgeID, _EdgeDecision],
    ) -> _ResponseReachability:
        variable_selectors = self._get_response_variable_selectors(response_node_id)
        blocking_edge_ids = {
            edge_id
            for edge_id in map(EdgeID, self.graph.edges)
            if self._is_blocking_edge(edge_id, variable_selectors)
        }
        return _ResponseReachability(
            self.graph,
            response_node_id,
            blocking_edge_ids,
            first_edge_decisions,
        )

    def _get_response_variable_selectors(
        self,
        response_node_id: NodeID,
    ) -> set[_Selector]:
        response_node = self.graph.nodes[response_node_id]
        response_session = _ResponseSession.from_node(response_node)
        return {
            tuple(segment.selector[:2])
            for segment in response_session.template.segments
            if isinstance(segment, VariableSegment)
        }

    def _is_blocking_edge(
        self,
        edge_id: EdgeID,
        variable_selectors: set[_Selector],
    ) -> bool:
        edge = self.graph.edges[edge_id]
        source_node = self.graph.nodes[edge.tail]
        if (
            source_node.promoted_to_branch
            and edge.source_handle != FailBranchSourceHandle.FAILED
        ):
            # A node promoted to BRANCH only because its FAIL_BRANCH error
            # strategy reroutes failures still streams its normal-path output
            # while it runs. Blocking it until completion would hold every
            # chunk back and burst-deliver them at the end of the node run
            # (dify#43938). Only the fail-branch edge stays blocking: it
            # resolves when the node actually fails.
            return False
        return source_node.execution_type in frozenset((
            NodeExecutionType.BRANCH,
            NodeExecutionType.CONTAINER,
            NodeExecutionType.RESPONSE,
        )) or source_node.blocks_variable_output(variable_selectors)

    def activate_initial_sessions(self) -> list[EngineEvent]:
        events: list[EngineEvent] = []
        for response_node_id in sorted(self._response_node_ids):
            if self._is_response_ready(response_node_id):
                events.extend(self._activate_or_queue_session(response_node_id))
        return events

    def _is_response_ready(self, response_node_id: NodeID) -> bool:
        gating_state = self._gating_state
        if isinstance(gating_state, _LegacyPathState):
            return any(
                path.is_empty()
                for path in gating_state.paths_by_response.get(response_node_id, [])
            )
        return gating_state.reachability_by_response[response_node_id].is_ready()

    def _handle_edge_taken(self, edge_id: EdgeID) -> list[EngineEvent]:
        events: list[EngineEvent] = []
        gating_state = self._gating_state
        if isinstance(gating_state, _ReachabilityState) and edge_id in self.graph.edges:
            gating_state.first_edge_decisions.setdefault(edge_id, _EdgeDecision.TAKEN)
        for response_node_id in sorted(self._response_node_ids):
            if isinstance(gating_state, _LegacyPathState):
                for path in gating_state.paths_by_response.get(response_node_id, []):
                    path.remove_edge(edge_id)
                ready = self._is_response_ready(response_node_id)
            else:
                ready = gating_state.reachability_by_response[
                    response_node_id
                ].advance_from_edge(edge_id)
            if ready:
                events.extend(self._activate_or_queue_session(response_node_id))
        return events

    def _activate_or_queue_session(
        self,
        node_id: NodeID,
    ) -> list[EngineEvent]:
        session = self._response_sessions.pop(node_id, None)
        if session is None:
            return []

        self._replies[node_id] = self._reply_order.add_reply(self.frame_id)
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
            and self._has_runnable_reasoning_source(NodeID(event.node_id))
            and self._is_reasoning_referenced_by_reached_session(event)
        )

    @staticmethod
    def _has_valid_reasoning_selector(event: NodeRunReasoningChunkEvent) -> bool:
        return tuple(event.selector) == (event.node_id, "reasoning_content")

    def _has_runnable_reasoning_source(self, node_id: NodeID) -> bool:
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

        response_node_id = NodeID(self._active_session.node_id)
        events = self._template_stream.flush(
            self._active_session,
            graph=self.graph,
            variable_pool=self.variable_pool,
        )
        if events:
            self._active_session.emitted = True
        self._replies[response_node_id].events.extend(events)
        if self._active_session.is_complete():
            self._end_session(response_node_id)

        return []

    def _end_session(self, node_id: NodeID) -> None:
        if not self._active_session or self._active_session.node_id != node_id:
            return

        self._close_active_session()
        if self._waiting_sessions:
            self._active_session = self._waiting_sessions.popleft()
            self._try_flush()

    def _close_active_session(self) -> None:
        if self._active_session is None:
            return
        node_id = NodeID(self._active_session.node_id)
        reply = self._replies[node_id]
        reply.events.extend(
            self._template_stream.finish_response(
                self._active_session, graph=self.graph
            )
        )
        reply.finished = True
        if reply.displayed or self.frame_id == ROOT_FRAME_ID:
            del self._replies[node_id]
        else:
            self._completed_sessions[node_id] = self._active_session
        self._active_session = None

    def _abandon_sessions_for_failed_node(self, node_id: NodeID) -> list[EngineEvent]:
        """Drop sessions whose source node failed before finishing its output.

        Only promoted fail-branch nodes apply: their normal-path sessions start
        before the node completes (see ``_is_blocking_edge``), and a failure
        abandons that path, so those sessions must stop waiting and free the
        active slot for the fail-branch session. Sessions of other nodes keep
        today's behavior.

        Args:
            node_id: ID of the node whose run raised.

        Returns:
            Events produced while releasing the active session.

        """
        source_node = self.graph.nodes.get(node_id)
        if source_node is None or not source_node.promoted_to_branch:
            return []

        abandoned_active = False
        if self._active_session is not None and self._session_references_node(
            self._active_session, node_id
        ):
            self._close_active_session()
            abandoned_active = True

        self._waiting_sessions = deque(
            session
            for session in self._waiting_sessions
            if not self._session_references_node(session, node_id)
        )
        if not abandoned_active:
            return []
        if self._waiting_sessions:
            self._active_session = self._waiting_sessions.popleft()
        return self._try_flush()

    @staticmethod
    def _session_references_node(session: _ResponseSession, node_id: NodeID) -> bool:
        return any(
            isinstance(segment, VariableSegment) and segment.selector[:1] == [node_id]
            for segment in session.template.segments
        )

    def _release_displayed_sessions(self) -> None:
        for node_id in list(self._completed_sessions):
            if self._replies[node_id].displayed:
                del self._completed_sessions[node_id]
                del self._replies[node_id]

    def _restart_hidden_replies(self, event: NodeRunRetryEvent) -> None:
        if self.frame_id == ROOT_FRAME_ID:
            return
        previous_attempt_streams = [
            source
            for (
                execution_id,
                attempt,
                _,
            ), source in self._template_stream.source_streams.items()
            if execution_id == event.id and attempt < event.retry_index
        ]
        pending_sessions = {
            session.node_id: session for session in self._reached_sessions()
        }
        sessions = {**self._completed_sessions, **pending_sessions}
        for node_id, session in sessions.items():
            reply = self._replies[node_id]
            replaced_segment_indices = [
                index
                for index, source in session.segment_streams.items()
                if any(
                    source is previous_stream
                    for previous_stream in previous_attempt_streams
                )
            ]
            if reply.displayed or not replaced_segment_indices:
                continue
            for index in replaced_segment_indices:
                del session.segment_streams[index]
            session.index = 0
            session.segment_positions.clear()
            session.emitted = False
            reply.events.clear()
            reply.finished = False
            self._completed_sessions.pop(node_id, None)
            pending_sessions[node_id] = session
        ordered_sessions = [
            pending_sessions[node_id]
            for node_id in self._replies
            if node_id in pending_sessions
        ]
        self._active_session = ordered_sessions[0] if ordered_sessions else None
        self._waiting_sessions = deque(ordered_sessions[1:])

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
