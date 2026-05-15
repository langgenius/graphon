from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal, cast
from uuid import uuid4

from pydantic import BaseModel, Field

from graphon.enums import NodeExecutionType, NodeState
from graphon.graph_engine.filters.base import GraphEventFilterContext
from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import GraphRunStartedEvent
from graphon.graph_events.node import (
    NodeRunExceptionEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.graph_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.nodes.base.template import Template, TextSegment, VariableSegment
from graphon.runtime.graph_runtime_state import GraphProtocol, NodeProtocol
from graphon.runtime.graph_runtime_state_protocol import ReadOnlyGraphRuntimeState

type NodeID = str
type EdgeID = str


@dataclass
class Path:
    """Blocking traversal edges that must be taken before a response can stream."""

    edges: list[EdgeID] = field(default_factory=list)

    def contains_edge(self, edge_id: EdgeID) -> bool:
        return edge_id in self.edges

    def remove_edge(self, edge_id: EdgeID) -> None:
        if self.contains_edge(edge_id):
            self.edges.remove(edge_id)

    def is_empty(self) -> bool:
        return len(self.edges) == 0


@dataclass
class ResponseSession:
    """Streaming cursor for one response node template."""

    node_id: str
    template: Template
    index: int = 0

    @classmethod
    def from_node(cls, node: NodeProtocol) -> ResponseSession:
        get_streaming_template = getattr(node, "get_streaming_template", None)
        if not callable(get_streaming_template):
            msg = (
                "ResponseSession.from_node requires "
                "get_streaming_template() on response nodes"
            )
            raise TypeError(msg)
        return cls(node_id=node.id, template=get_streaming_template())

    def is_complete(self) -> bool:
        return self.index >= len(self.template.segments)


class ResponseSessionState(BaseModel):
    """Serializable representation of a response session."""

    node_id: str
    index: int = Field(default=0, ge=0)


class StreamBufferState(BaseModel):
    """Serializable representation of buffered stream chunks."""

    selector: tuple[str, ...]
    events: list[NodeRunStreamChunkEvent] = Field(default_factory=list)


class StreamPositionState(BaseModel):
    """Serializable representation for stream read positions."""

    selector: tuple[str, ...]
    position: int = Field(default=0, ge=0)


class ResponseStreamFilterState(BaseModel):
    """Serialized snapshot of ResponseStreamFilter."""

    type: Literal["ResponseStreamFilter"] = Field(default="ResponseStreamFilter")
    version: str = Field(default="1.0")
    response_nodes: Sequence[str] = Field(default_factory=list)
    active_session: ResponseSessionState | None = None
    waiting_sessions: Sequence[ResponseSessionState] = Field(default_factory=list)
    pending_sessions: Sequence[ResponseSessionState] = Field(default_factory=list)
    node_execution_ids: dict[str, str] = Field(default_factory=dict)
    paths_map: dict[str, list[list[str]]] = Field(default_factory=dict)
    stream_buffers: Sequence[StreamBufferState] = Field(default_factory=list)
    stream_positions: Sequence[StreamPositionState] = Field(default_factory=list)
    closed_streams: Sequence[tuple[str, ...]] = Field(default_factory=list)


class ResponseStreamFilter:
    """Opt-in event filter that recreates legacy ordered response streaming."""

    filter_id = "graphon.response_stream.v1"

    def __init__(self, *, pass_unmatched_chunks: bool = False) -> None:
        self._pass_unmatched_chunks = pass_unmatched_chunks
        self._context: GraphEventFilterContext | None = None
        self._graph: GraphProtocol | None = None
        self._runtime_state: ReadOnlyGraphRuntimeState | None = None
        self._active_session: ResponseSession | None = None
        self._waiting_sessions: deque[ResponseSession] = deque()
        self._stream_buffers: dict[tuple[str, ...], list[NodeRunStreamChunkEvent]] = {}
        self._stream_positions: dict[tuple[str, ...], int] = {}
        self._closed_streams: set[tuple[str, ...]] = set()
        self._response_nodes: set[str] = set()
        self._paths_maps: dict[str, list[Path]] = {}
        self._node_execution_ids: dict[str, str] = {}
        self._response_sessions: dict[str, ResponseSession] = {}
        self._referenced_selectors: set[tuple[str, ...]] = set()

    def initialize(self, context: GraphEventFilterContext) -> None:
        self._context = context
        self._graph = cast(GraphProtocol, context.graph)
        self._runtime_state = context.runtime_state
        for node in context.graph.nodes.values():
            if node.execution_type == NodeExecutionType.RESPONSE:
                self._register(node.id)

    def on_event(self, event: GraphEngineEvent) -> Iterable[GraphEngineEvent]:
        match event:
            case GraphRunStartedEvent():
                output: Iterable[GraphEngineEvent] = [
                    event,
                    *self._activate_initial_sessions(),
                ]
            case NodeRunStartedEvent():
                self._node_execution_ids[event.node_id] = event.id
                output = [event]
            case NodeRunStreamChunkEvent():
                output = self._handle_stream_chunk(event)
            case GraphEdgeTakenEvent():
                output = self._handle_edge_taken(event.edge_id)
            case GraphEdgeSkippedEvent():
                output = []
            case NodeRunSucceededEvent() | NodeRunExceptionEvent():
                output = [*self._try_flush(), event]
            case _:
                output = [event]
        return output

    def flush(self) -> Iterable[GraphEngineEvent]:
        return self._try_flush()

    def dumps(self) -> str:
        state = ResponseStreamFilterState(
            response_nodes=sorted(self._response_nodes),
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
            node_execution_ids=dict(sorted(self._node_execution_ids.items())),
            paths_map={
                node_id: [path.edges.copy() for path in paths]
                for node_id, paths in sorted(self._paths_maps.items())
            },
            stream_buffers=[
                StreamBufferState(
                    selector=selector,
                    events=[event.model_copy(deep=True) for event in events],
                )
                for selector, events in sorted(self._stream_buffers.items())
            ],
            stream_positions=[
                StreamPositionState(selector=selector, position=position)
                for selector, position in sorted(self._stream_positions.items())
            ],
            closed_streams=sorted(self._closed_streams),
        )
        return state.model_dump_json()

    def loads(self, data: str) -> None:
        state = ResponseStreamFilterState.model_validate_json(data)

        if state.type != "ResponseStreamFilter":
            msg = f"Invalid serialized data type: {state.type}"
            raise ValueError(msg)

        if state.version != "1.0":
            msg = f"Unsupported serialized version: {state.version}"
            raise ValueError(msg)

        self._response_nodes = set(state.response_nodes)
        self._paths_maps = {
            node_id: [Path(edges=list(path_edges)) for path_edges in paths]
            for node_id, paths in state.paths_map.items()
        }
        self._node_execution_ids = dict(state.node_execution_ids)

        self._stream_buffers = {
            tuple(buffer.selector): [
                event.model_copy(deep=True) for event in buffer.events
            ]
            for buffer in state.stream_buffers
        }
        self._stream_positions = {
            tuple(position.selector): position.position
            for position in state.stream_positions
        }
        for selector in self._stream_buffers:
            self._stream_positions.setdefault(selector, 0)

        self._closed_streams = {tuple(selector) for selector in state.closed_streams}
        self._waiting_sessions = deque(
            self._session_from_state(session_state)
            for session_state in state.waiting_sessions
        )
        self._response_sessions = {
            session_state.node_id: self._session_from_state(session_state)
            for session_state in state.pending_sessions
        }
        self._active_session = (
            self._session_from_state(state.active_session)
            if state.active_session
            else None
        )

        self._referenced_selectors = set()
        for response_node_id in self._response_nodes:
            self._record_referenced_selectors(response_node_id)

    @property
    def _bound_graph(self) -> GraphProtocol:
        if self._graph is None:
            msg = "ResponseStreamFilter must be initialized before use."
            raise RuntimeError(msg)
        return self._graph

    @property
    def _bound_runtime_state(self) -> ReadOnlyGraphRuntimeState:
        if self._runtime_state is None:
            msg = "ResponseStreamFilter must be initialized before use."
            raise RuntimeError(msg)
        return self._runtime_state

    def _register(self, response_node_id: NodeID) -> None:
        if response_node_id in self._response_nodes:
            return
        self._response_nodes.add(response_node_id)
        self._paths_maps[response_node_id] = self._build_paths_map(response_node_id)

        response_node = self._bound_graph.nodes[response_node_id]
        self._response_sessions[response_node_id] = ResponseSession.from_node(
            response_node,
        )
        self._record_referenced_selectors(response_node_id)

    def _record_referenced_selectors(self, response_node_id: NodeID) -> None:
        response_node = self._bound_graph.nodes.get(response_node_id)
        if response_node is None:
            return

        response_session = ResponseSession.from_node(response_node)
        for segment in response_session.template.segments:
            if isinstance(segment, VariableSegment):
                self._referenced_selectors.add(tuple(segment.selector))

    def _build_paths_map(self, response_node_id: NodeID) -> list[Path]:
        root_node_id = self._bound_graph.root_node.id
        if root_node_id == response_node_id:
            return [Path()]

        variable_selectors = self._get_response_variable_selectors(response_node_id)
        all_complete_paths = self._find_all_paths(root_node_id, response_node_id)
        return [
            Path(edges=self._get_blocking_edges(path, variable_selectors))
            for path in all_complete_paths
        ]

    def _get_response_variable_selectors(
        self,
        response_node_id: NodeID,
    ) -> set[tuple[str, ...]]:
        response_node = self._bound_graph.nodes[response_node_id]
        response_session = ResponseSession.from_node(response_node)
        return {
            tuple(segment.selector[:2])
            for segment in response_session.template.segments
            if isinstance(segment, VariableSegment)
        }

    def _find_all_paths(
        self,
        current_node_id: NodeID,
        target_node_id: NodeID,
        current_path: list[EdgeID] | None = None,
        visited: set[NodeID] | None = None,
    ) -> list[list[EdgeID]]:
        current_path = current_path or []
        visited = visited or set()
        if current_node_id == target_node_id:
            return [current_path.copy()]

        next_visited = {current_node_id, *visited}
        paths: list[list[EdgeID]] = []
        for edge in self._bound_graph.get_outgoing_edges(current_node_id):
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
        path: list[EdgeID],
        variable_selectors: set[tuple[str, ...]],
    ) -> list[EdgeID]:
        return [
            edge_id
            for edge_id in path
            if self._is_blocking_edge(edge_id, variable_selectors)
        ]

    def _is_blocking_edge(
        self,
        edge_id: EdgeID,
        variable_selectors: set[tuple[str, ...]],
    ) -> bool:
        edge = self._bound_graph.edges[edge_id]
        source_node = self._bound_graph.nodes[edge.tail]
        return source_node.execution_type in frozenset((
            NodeExecutionType.BRANCH,
            NodeExecutionType.CONTAINER,
            NodeExecutionType.RESPONSE,
        )) or source_node.blocks_variable_output(variable_selectors)

    def _activate_initial_sessions(self) -> list[GraphEngineEvent]:
        events: list[GraphEngineEvent] = []
        for response_node_id in sorted(self._response_nodes):
            paths = self._paths_maps.get(response_node_id, [])
            if any(path.is_empty() for path in paths):
                events.extend(self._active_or_queue_session(response_node_id))
        return events

    def _handle_edge_taken(self, edge_id: EdgeID) -> list[GraphEngineEvent]:
        events: list[GraphEngineEvent] = []
        for response_node_id in sorted(self._response_nodes):
            paths = self._paths_maps.get(response_node_id)
            if paths is None:
                continue

            has_reachable_path = False
            for path in paths:
                path.remove_edge(edge_id)
                if path.is_empty():
                    has_reachable_path = True

            if has_reachable_path:
                events.extend(self._active_or_queue_session(response_node_id))
        return events

    def _active_or_queue_session(
        self,
        node_id: NodeID,
    ) -> list[GraphEngineEvent]:
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
    ) -> list[GraphEngineEvent]:
        selector_key = tuple(event.selector)
        if selector_key in self._referenced_selectors:
            self._append_stream_chunk(event.selector, event)
            if event.is_final:
                self._close_stream(event.selector)
            return self._try_flush()
        if self._pass_unmatched_chunks:
            return [event]
        return []

    def _get_or_create_execution_id(self, node_id: NodeID) -> str:
        if node_id not in self._node_execution_ids:
            self._node_execution_ids[node_id] = str(uuid4())
        return self._node_execution_ids[node_id]

    def _create_stream_chunk_event(
        self,
        node_id: NodeID,
        execution_id: str,
        selector: Sequence[str],
        chunk: str,
        is_final: bool = False,
    ) -> NodeRunStreamChunkEvent:
        graph = self._bound_graph
        if selector and selector[0] not in graph.nodes and self._active_session:
            response_node = graph.nodes[self._active_session.node_id]
            return NodeRunStreamChunkEvent(
                id=execution_id,
                node_id=response_node.id,
                node_type=response_node.node_type,
                selector=list(selector),
                chunk=chunk,
                is_final=is_final,
            )

        node = graph.nodes[node_id]
        return NodeRunStreamChunkEvent(
            id=execution_id,
            node_id=node.id,
            node_type=node.node_type,
            selector=list(selector),
            chunk=chunk,
            is_final=is_final,
        )

    def _process_variable_segment(
        self,
        segment: VariableSegment,
    ) -> tuple[list[NodeRunStreamChunkEvent], bool]:
        events: list[NodeRunStreamChunkEvent] = []
        source_selector_prefix = segment.selector[0] if segment.selector else ""
        is_complete = False

        is_special_selector = source_selector_prefix not in self._bound_graph.nodes
        if self._active_session and is_special_selector:
            output_node_id = self._active_session.node_id
        else:
            output_node_id = source_selector_prefix
        execution_id = self._get_or_create_execution_id(output_node_id)

        while self._has_unread_stream(segment.selector):
            event = self._pop_stream_chunk(segment.selector)
            if event is None:
                continue

            if self._active_session and is_special_selector:
                response_node = self._bound_graph.nodes[self._active_session.node_id]
                events.append(
                    NodeRunStreamChunkEvent(
                        id=execution_id,
                        node_id=response_node.id,
                        node_type=response_node.node_type,
                        selector=list(event.selector),
                        chunk=event.chunk,
                        is_final=event.is_final,
                    )
                )
            else:
                events.append(event)

        if self._is_stream_closed(segment.selector):
            is_complete = True
        elif value := self._bound_runtime_state.variable_pool.get(segment.selector):
            is_last_segment = bool(
                self._active_session
                and self._active_session.index
                == len(self._active_session.template.segments) - 1,
            )
            events.append(
                self._create_stream_chunk_event(
                    node_id=output_node_id,
                    execution_id=execution_id,
                    selector=segment.selector,
                    chunk=value.markdown,
                    is_final=is_last_segment,
                ),
            )
            is_complete = True

        return events, is_complete

    def _process_text_segment(
        self,
        segment: TextSegment,
    ) -> list[NodeRunStreamChunkEvent]:
        active_session = self._active_session
        if active_session is None:
            msg = "Cannot process a text segment without an active response session."
            raise RuntimeError(msg)

        current_response_node = self._bound_graph.nodes[active_session.node_id]
        execution_id = self._get_or_create_execution_id(current_response_node.id)
        is_last_segment = (
            active_session.index == len(active_session.template.segments) - 1
        )
        return [
            self._create_stream_chunk_event(
                node_id=current_response_node.id,
                execution_id=execution_id,
                selector=self._get_text_segment_selector(current_response_node.id),
                chunk=segment.text,
                is_final=is_last_segment,
            )
        ]

    def _get_text_segment_selector(self, response_node_id: NodeID) -> Sequence[str]:
        response_node = self._bound_graph.nodes[response_node_id]
        get_streaming_text_selector = getattr(
            response_node,
            "get_streaming_text_selector",
            None,
        )
        if callable(get_streaming_text_selector):
            selector = get_streaming_text_selector()
            return [str(part) for part in selector]
        return [response_node.id, "answer"]

    def _try_flush(self) -> list[GraphEngineEvent]:
        if not self._active_session:
            return []

        template = self._active_session.template
        response_node_id = self._active_session.node_id
        events: list[GraphEngineEvent] = []

        while self._active_session.index < len(template.segments):
            segment = template.segments[self._active_session.index]

            if isinstance(segment, VariableSegment):
                source_selector_prefix = segment.selector[0] if segment.selector else ""
                if source_selector_prefix in self._bound_graph.nodes:
                    source_node = self._bound_graph.nodes[source_selector_prefix]
                    if source_node.state == NodeState.SKIPPED:
                        self._active_session.index += 1
                        continue

                segment_events, is_complete = self._process_variable_segment(segment)
                events.extend(segment_events)

                if is_complete:
                    self._active_session.index += 1
                else:
                    break
            else:
                events.extend(self._process_text_segment(segment))
                self._active_session.index += 1

        if self._active_session.is_complete():
            events.extend(self._end_session(response_node_id))

        return events

    def _end_session(self, node_id: NodeID) -> list[GraphEngineEvent]:
        if not self._active_session or self._active_session.node_id != node_id:
            return []

        self._active_session = None
        if not self._waiting_sessions:
            return []

        self._active_session = self._waiting_sessions.popleft()
        return self._try_flush()

    def _append_stream_chunk(
        self,
        selector: Sequence[str],
        event: NodeRunStreamChunkEvent,
    ) -> None:
        key = tuple(selector)
        if key in self._closed_streams:
            msg = f"Stream {'.'.join(selector)} is already closed"
            raise ValueError(msg)

        if key not in self._stream_buffers:
            self._stream_buffers[key] = []
            self._stream_positions[key] = 0

        self._stream_buffers[key].append(event)

    def _pop_stream_chunk(
        self,
        selector: Sequence[str],
    ) -> NodeRunStreamChunkEvent | None:
        key = tuple(selector)
        if key not in self._stream_buffers:
            return None

        position = self._stream_positions.get(key, 0)
        buffer = self._stream_buffers[key]
        if position >= len(buffer):
            return None

        event = buffer[position]
        self._stream_positions[key] = position + 1
        return event

    def _has_unread_stream(self, selector: Sequence[str]) -> bool:
        key = tuple(selector)
        if key not in self._stream_buffers:
            return False

        position = self._stream_positions.get(key, 0)
        return position < len(self._stream_buffers[key])

    def _close_stream(self, selector: Sequence[str]) -> None:
        self._closed_streams.add(tuple(selector))

    def _is_stream_closed(self, selector: Sequence[str]) -> bool:
        return tuple(selector) in self._closed_streams

    def _serialize_session(
        self,
        session: ResponseSession | None,
    ) -> ResponseSessionState | None:
        if session is None:
            return None
        return ResponseSessionState(node_id=session.node_id, index=session.index)

    def _session_from_state(
        self,
        session_state: ResponseSessionState,
    ) -> ResponseSession:
        node = self._bound_graph.nodes.get(session_state.node_id)
        if node is None:
            msg = f"Unknown response node '{session_state.node_id}' in serialized state"
            raise ValueError(msg)

        session = ResponseSession.from_node(node)
        session.index = session_state.index
        return session
