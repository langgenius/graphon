"""Complete frame, reachability, and reply progress for paired pause snapshots."""

from __future__ import annotations

# Version codecs own the wire format and intentionally serialize package internals.
# ruff: file-ignore[private-member-access]
from collections import deque
from typing import TYPE_CHECKING, Annotated, Literal, Self, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from graphon.engine.filter.protocol import EngineEventFilterContext
from graphon.engine_events.frame import FrameStartedEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.nodes.base.template import Template, VariableSegment
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol
from graphon.runtime.variable_pool import VariablePool

from .frame_graph import build_child_graph
from .frame_session import (
    _BlockingPath,
    _FrameSession,
    _LegacyPathState,
    _ReachabilityState,
)
from .ids import EdgeID, NodeID
from .reachability import _EdgeDecision
from .reply_order import _ContainerReplies, _FrameReplies, _Reply, _ReplyOrder
from .snapshot import Selector, SnapshotHeader, StreamBufferState, StreamPositionState
from .template_stream import (
    _ResponseSession,
    _SourceStream,
    _StreamBuffers,
    _TemplateStream,
)

if TYPE_CHECKING:
    from .filter import ResponseStreamFilter

Index = Annotated[int, Field(ge=0)]


class SourceStreamState(BaseModel):
    events: list[NodeRunStreamChunkEvent]
    closed: bool


class SourceKey(BaseModel):
    execution_id: str
    attempt: Index
    selector: Selector
    source: Index


class LatestSource(BaseModel):
    selector: Selector
    source: Index


class ResponseSessionState(BaseModel):
    node_id: str
    template: Template
    index: Index
    segment_streams: dict[Index, Index]
    segment_positions: dict[Index, Index]
    emitted: bool


class FrameSessionState(BaseModel):
    start: FrameStartedEvent | None
    values: VariablePool | None
    response_nodes: list[NodeID]
    active_session: ResponseSessionState | None
    waiting_sessions: list[ResponseSessionState]
    pending_sessions: list[ResponseSessionState]
    completed_sessions: list[ResponseSessionState]
    replies: dict[str, Index]
    paths_map: dict[NodeID, list[list[EdgeID]]] | None = None
    taken_edges: list[EdgeID] = Field(default_factory=list)
    rejected_edges: list[EdgeID] = Field(default_factory=list)
    skipped_edges: list[EdgeID] | None
    node_execution_ids: dict[str, str]
    stream_buffers: list[StreamBufferState]
    stream_positions: list[StreamPositionState]
    closed_streams: list[Selector]
    sources: list[SourceStreamState]
    source_keys: list[SourceKey]
    latest_sources: list[LatestSource]
    finished_nodes: list[str]

    @model_validator(mode="after")
    def _reject_mixed_gating_modes(self) -> Self:
        if self.paths_map is not None and (self.taken_edges or self.rejected_edges):
            msg = "Legacy paths cannot be combined with native edge decisions"
            raise ValueError(msg)
        return self


class ReplyState(BaseModel):
    events: list[NodeRunStreamChunkEvent]
    finished: bool
    displayed: bool


class FrameRepliesState(BaseModel):
    frame_id: str
    entries: list[Index | ContainerRepliesState]
    finished: bool
    failed: bool
    waiting_after_failure: bool


class ContainerRepliesState(BaseModel):
    container_key: tuple[str, str]
    invocation_id: str | None
    frames: dict[Index, FrameRepliesState]
    next_index: Index
    unindexed_frame_count: Index
    finished: bool
    failed: bool
    waiting_after_failure: bool
    current: bool


class Snapshot(SnapshotHeader[Literal["4.0"]]):
    model_config = ConfigDict(extra="forbid")

    version: Literal["4.0"] = "4.0"
    run_id: str | None
    frames: dict[str, FrameSessionState]
    replies: list[ReplyState]
    order: FrameRepliesState
    awaiting_root_baseline: bool
    suppress_children: bool
    started: bool
    finished_frames: list[str]


def loads(data: str) -> Snapshot:
    return Snapshot.model_validate_json(data)


def dumps(state: SnapshotHeader) -> str:
    if not isinstance(state, Snapshot):
        msg = "Expected a version 4 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    return state.model_dump_json()


def capture(response_filter: ResponseStreamFilter) -> Snapshot:
    replies: list[ReplyState] = []
    reply_indices: dict[int, int] = {}

    def save_reply(reply: _Reply) -> int:
        if id(reply) not in reply_indices:
            reply_indices[id(reply)] = len(replies)
            replies.append(
                ReplyState(
                    events=list(reply.events),
                    finished=reply.finished,
                    displayed=reply.displayed,
                )
            )
        return reply_indices[id(reply)]

    def save_frame_replies(frame: _FrameReplies) -> FrameRepliesState:
        return FrameRepliesState(
            frame_id=frame.frame_id,
            entries=[
                save_reply(entry)
                if isinstance(entry, _Reply)
                else save_container_replies(entry)
                for entry in frame.entries
            ],
            finished=frame.finished,
            failed=frame.failed,
            waiting_after_failure=frame.waiting_after_failure,
        )

    def save_container_replies(container: _ContainerReplies) -> ContainerRepliesState:
        return ContainerRepliesState(
            container_key=container.container_key,
            invocation_id=container.invocation_id,
            frames={
                index: save_frame_replies(frame)
                for index, frame in container.frames.items()
            },
            next_index=container.next_index,
            unindexed_frame_count=container.unindexed_frame_count,
            finished=container.finished,
            failed=container.failed,
            waiting_after_failure=container.waiting_after_failure,
            current=response_filter._reply_order._containers.get(
                container.container_key
            )
            is container,
        )

    order = save_frame_replies(response_filter._reply_order._frames[ROOT_FRAME_ID])
    frames = {
        frame_id: _capture_frame_session(
            session,
            {node_id: save_reply(reply) for node_id, reply in session._replies.items()},
        )
        for frame_id, session in response_filter._frame_sessions.items()
    }
    if ROOT_FRAME_ID not in frames:
        frames[ROOT_FRAME_ID] = _capture_frame_session(
            response_filter._initialized_root_session, {}
        )
    return Snapshot(
        run_id=response_filter._run_id,
        frames=frames,
        replies=replies,
        order=order,
        awaiting_root_baseline=response_filter._awaiting_root_baseline,
        suppress_children=response_filter._suppress_child_responses,
        started=response_filter._run_started,
        finished_frames=sorted(response_filter._finished_frame_ids),
    )


def _capture_frame_session(
    session: _FrameSession, replies: dict[str, int]
) -> FrameSessionState:
    stream = session._template_stream
    sources: list[SourceStreamState] = []
    source_indices: dict[int, int] = {}

    def save_source(source: _SourceStream) -> int:
        if id(source) not in source_indices:
            source_indices[id(source)] = len(sources)
            sources.append(
                SourceStreamState(events=source.events, closed=source.closed)
            )
        return source_indices[id(source)]

    def save_response(response: _ResponseSession) -> ResponseSessionState:
        return ResponseSessionState(
            node_id=response.node_id,
            template=response.template,
            index=response.index,
            segment_streams={
                index: save_source(source)
                for index, source in response.segment_streams.items()
            },
            segment_positions=response.segment_positions,
            emitted=response.emitted,
        )

    active = save_response(session._active_session) if session._active_session else None
    waiting = [save_response(response) for response in session._waiting_sessions]
    pending = [
        save_response(response) for response in session._response_sessions.values()
    ]
    completed = [
        save_response(response) for response in session._completed_sessions.values()
    ]
    source_keys = [
        SourceKey(
            execution_id=execution_id,
            attempt=attempt,
            selector=selector,
            source=save_source(source),
        )
        for (execution_id, attempt, selector), source in stream.source_streams.items()
    ]
    latest_sources = [
        LatestSource(selector=selector, source=save_source(source))
        for selector, source in stream.latest_streams.items()
    ]
    gating_state = session._gating_state
    if isinstance(gating_state, _ReachabilityState):
        paths_map = None
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
    else:
        paths_map = {
            node_id: [path.edge_ids for path in paths]
            for node_id, paths in gating_state.paths_by_response.items()
        }
        taken_edges = []
        rejected_edges = []
    return FrameSessionState(
        start=session.start_event,
        values=session._observed_values,
        response_nodes=sorted(session._response_node_ids),
        active_session=active,
        waiting_sessions=waiting,
        pending_sessions=pending,
        completed_sessions=completed,
        replies=replies,
        paths_map=paths_map,
        taken_edges=taken_edges,
        rejected_edges=rejected_edges,
        skipped_edges=sorted(session._skipped_edge_ids)
        if session._skipped_edge_ids is not None
        else None,
        node_execution_ids=stream.node_execution_ids,
        stream_buffers=stream.stream_buffers.dump_buffers(),
        stream_positions=stream.stream_buffers.dump_positions(),
        closed_streams=stream.stream_buffers.dump_closed_selectors(),
        sources=sources,
        source_keys=source_keys,
        latest_sources=latest_sources,
        finished_nodes=sorted(stream.finished_node_ids),
    )


def restore(
    state: Snapshot, context: EngineEventFilterContext, *, pass_unmatched_chunks: bool
) -> tuple[dict[str, _FrameSession], _ReplyOrder]:
    if state.run_id != context.run_id:
        msg = "ResponseStreamFilter snapshot belongs to a different run"
        raise ValueError(msg)
    if ROOT_FRAME_ID not in state.frames or state.order.frame_id != ROOT_FRAME_ID:
        msg = "Missing root response progress"
        raise ValueError(msg)
    if state.frames.keys() & set(state.finished_frames):
        msg = "Finished frames cannot contain active response progress"
        raise ValueError(msg)
    replies = [
        _Reply(deque(reply.events), reply.finished, reply.displayed)
        for reply in state.replies
    ]
    order = _restore_order(state.order, replies)
    sessions: dict[str, _FrameSession] = {}
    for frame_id, saved in state.frames.items():
        graph = (
            cast(GraphProtocol, context.graph)
            if frame_id == ROOT_FRAME_ID
            else (
                build_child_graph(
                    getattr(context.graph, "graph_config", None), saved.start
                )
                if saved.start
                else None
            )
        )
        if (
            graph is None
            or frame_id not in order._frames
            or (saved.start is not None and saved.start.frame_id != frame_id)
        ):
            msg = f"Missing or mismatched response progress for frame {frame_id}"
            raise ValueError(msg)
        session = _FrameSession(
            graph,
            context.runtime_state.variable_pool if frame_id == ROOT_FRAME_ID else None,
            reply_order=order,
            frame_id=frame_id,
            container_id=saved.start.container_id if saved.start else "",
            pass_unmatched_chunks=pass_unmatched_chunks,
        )
        _restore_frame_session(session, saved, replies)
        sessions[frame_id] = session
    return sessions, order


def _restore_order(
    saved_order: FrameRepliesState, replies: list[_Reply]
) -> _ReplyOrder:
    order = _ReplyOrder()
    order._frames.clear()
    queued_replies: set[int] = set()

    def restore_frame_replies(saved: FrameRepliesState) -> _FrameReplies:
        if saved.frame_id in order._frames:
            msg = f"Duplicate response frame {saved.frame_id}"
            raise ValueError(msg)
        frame = _FrameReplies(
            saved.frame_id,
            finished=saved.finished,
            failed=saved.failed,
            waiting_after_failure=saved.waiting_after_failure,
        )
        order._frames[frame.frame_id] = frame
        for entry in saved.entries:
            if isinstance(entry, int):
                if entry >= len(replies) or entry in queued_replies:
                    msg = "Invalid pending reply reference"
                    raise ValueError(msg)
                queued_replies.add(entry)
                frame.entries.append(replies[entry])
            else:
                if entry.container_key[0] != frame.frame_id:
                    msg = "Mismatched container parent in response progress"
                    raise ValueError(msg)
                container = _ContainerReplies(
                    entry.container_key,
                    invocation_id=entry.invocation_id,
                    frames={
                        index: restore_frame_replies(child)
                        for index, child in entry.frames.items()
                    },
                    next_index=entry.next_index,
                    unindexed_frame_count=entry.unindexed_frame_count,
                    finished=entry.finished,
                    failed=entry.failed,
                    waiting_after_failure=entry.waiting_after_failure,
                )
                if entry.current:
                    if container.container_key in order._containers:
                        msg = "Duplicate current response container"
                        raise ValueError(msg)
                    order._containers[container.container_key] = container
                frame.entries.append(container)
        return frame

    restore_frame_replies(saved_order)
    return order


def _restore_frame_session(
    session: _FrameSession, saved: FrameSessionState, replies: list[_Reply]
) -> None:
    sources = [_SourceStream(source.events, source.closed) for source in saved.sources]

    session.start_event = saved.start
    session._observed_values = saved.values
    if saved.values is not None:
        session.variable_pool = saved.values
    session._response_node_ids = set(saved.response_nodes)
    for node_id in session._response_node_ids:
        if node_id not in session.graph.nodes:
            msg = f"Unknown response node '{node_id}' in serialized state"
            raise ValueError(msg)
    session._skipped_edge_ids = (
        set(saved.skipped_edges) if saved.skipped_edges is not None else None
    )
    if saved.paths_map is not None:
        session._gating_state = _LegacyPathState({
            node_id: [_BlockingPath(path) for path in paths]
            for node_id, paths in saved.paths_map.items()
        })
    else:
        first_edge_decisions = dict.fromkeys(saved.taken_edges, _EdgeDecision.TAKEN)
        first_edge_decisions.update(
            dict.fromkeys(saved.rejected_edges, _EdgeDecision.SKIPPED)
        )
        session._gating_state = _ReachabilityState(
            first_edge_decisions=first_edge_decisions,
            reachability_by_response={
                node_id: session._build_reachability(node_id, first_edge_decisions)
                for node_id in session._response_node_ids
            },
        )
    session._active_session = (
        _restore_response(saved.active_session, session.graph, sources)
        if saved.active_session
        else None
    )
    session._waiting_sessions = deque(
        _restore_response(response, session.graph, sources)
        for response in saved.waiting_sessions
    )
    session._response_sessions = {
        NodeID(response.node_id): _restore_response(response, session.graph, sources)
        for response in saved.pending_sessions
    }
    session._completed_sessions = {
        response.node_id: _restore_response(response, session.graph, sources)
        for response in saved.completed_sessions
    }
    if any(index >= len(replies) for index in saved.replies.values()):
        msg = "Invalid frame reply reference"
        raise ValueError(msg)
    session._replies = {
        node_id: replies[index] for node_id, index in saved.replies.items()
    }
    if any(
        response.node_id not in session._replies
        for response in (
            *session._reached_sessions(),
            *session._completed_sessions.values(),
        )
    ):
        msg = "Missing response reply progress"
        raise ValueError(msg)
    stream_buffers = _StreamBuffers.from_state(
        buffers=saved.stream_buffers,
        positions=saved.stream_positions,
        closed_selectors=saved.closed_streams,
    )
    if any(
        position > len(stream_buffers.events.get(selector, []))
        for selector, position in stream_buffers.positions.items()
    ):
        msg = "Invalid root stream position"
        raise ValueError(msg)
    session._template_stream = _TemplateStream(
        stream_buffers=stream_buffers,
        node_execution_ids=dict(saved.node_execution_ids),
        frame_id=session.frame_id,
        container_id=saved.start.container_id if saved.start else "",
        skipped_node_ids=session._find_skipped_node_ids(),
        source_streams={
            (
                source_key.execution_id,
                source_key.attempt,
                source_key.selector,
            ): _source_at(sources, source_key.source)
            for source_key in saved.source_keys
        },
        latest_streams={
            latest_source.selector: _source_at(sources, latest_source.source)
            for latest_source in saved.latest_sources
        },
        finished_node_ids=set(saved.finished_nodes),
    )
    session._referenced_selectors = {
        selector
        for node_id in session._response_node_ids
        for selector in session._get_referenced_selectors(node_id)
    }
    session._referenced_selectors.update({
        tuple(segment.selector)
        for response in (
            *session._reached_sessions(),
            *session._response_sessions.values(),
            *session._completed_sessions.values(),
        )
        for segment in response.template.segments
        if isinstance(segment, VariableSegment)
    })


def _restore_response(
    state: ResponseSessionState, graph: GraphProtocol, sources: list[_SourceStream]
) -> _ResponseSession:
    if state.node_id not in graph.nodes:
        msg = f"Unknown response node '{state.node_id}' in serialized state"
        raise ValueError(msg)
    if state.index > len(state.template.segments):
        msg = "Invalid response template position"
        raise ValueError(msg)
    for index, position in state.segment_positions.items():
        if (
            index >= len(state.template.segments)
            or index not in state.segment_streams
            or position > len(_source_at(sources, state.segment_streams[index]).events)
        ):
            msg = "Invalid response source position"
            raise ValueError(msg)
    return _ResponseSession(
        state.node_id,
        state.template,
        state.index,
        {
            index: _source_at(sources, source)
            for index, source in state.segment_streams.items()
        },
        dict(state.segment_positions),
        state.emitted,
    )


def _source_at(sources: list[_SourceStream], index: int) -> _SourceStream:
    if index >= len(sources):
        msg = "Invalid response source reference"
        raise ValueError(msg)
    return sources[index]
