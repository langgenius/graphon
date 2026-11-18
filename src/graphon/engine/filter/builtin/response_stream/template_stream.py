"""Assembly shared by ResponseStreamFilter's root and child frame sessions.

Frame sessions choose which template drains into the filter's response output.
Legacy root cursors remain snapshot-compatible; child occurrences read their own
source execution and attempt without consuming another response's text.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from itertools import islice
from uuid import uuid4

from graphon.engine_events.base import NodeEvent
from graphon.engine_events.node import (
    NodeRunRetryEvent,
    NodeRunStreamChunkEvent,
    ResponseRead,
)
from graphon.enums import BuiltinNodeTypes, NodeState
from graphon.nodes.base.template import Template, TextSegment, VariableSegment
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.runtime_state import GraphProtocol, NodeProtocol
from graphon.runtime.runtime_state_protocol import ReadOnlyVariablePool

from .snapshot import Selector, StreamBufferState, StreamPositionState


def _is_llm_text_chunk(event: NodeRunStreamChunkEvent) -> bool:
    return event.node_type == BuiltinNodeTypes.LLM and tuple(event.selector) == (
        event.node_id,
        "text",
    )


def _only_empty_llm_deltas_before_retry(
    chunks: Sequence[NodeRunStreamChunkEvent], next_attempt: NodeEvent
) -> bool:
    return all(
        not chunk.chunk and _is_delta_before_retry(chunk, next_attempt)
        for chunk in chunks
    )


def _is_delta_before_retry(
    chunk: NodeRunStreamChunkEvent, next_attempt: NodeEvent
) -> bool:
    return (
        _is_llm_text_chunk(chunk)
        and chunk.id == next_attempt.id
        and chunk.frame_id == next_attempt.frame_id
        and chunk.node_id == next_attempt.node_id
        and chunk.retry_index < next_attempt.retry_index
        and not chunk.is_final
    )


@dataclass
class _SourceStream:
    events: list[NodeRunStreamChunkEvent] = field(default_factory=list)
    closed: bool = False


@dataclass
class _ResponseSession:
    """Streaming cursor for one response node template."""

    node_id: str
    template: Template
    index: int = 0
    segment_streams: dict[int, _SourceStream] = field(default_factory=dict)
    segment_positions: dict[int, int] = field(default_factory=dict)
    emitted: bool = False
    llm_text_reads: dict[int, ResponseRead] = field(default_factory=dict)
    # A resumed frame resolves None to its saved execution status.
    completed_without_saved_reads: bool | None = False

    @classmethod
    def from_node(cls, node: NodeProtocol) -> _ResponseSession:
        get_streaming_template = getattr(node, "get_streaming_template", None)
        if not callable(get_streaming_template):
            msg = (
                "Response streaming requires get_streaming_template() on response nodes"
            )
            raise TypeError(msg)
        return cls(node_id=node.id, template=get_streaming_template())

    @property
    def selectors(self) -> set[Selector]:
        return {
            tuple(segment.selector)
            for segment in self.template.segments
            if isinstance(segment, VariableSegment)
        }

    def is_complete(self) -> bool:
        return self.index >= len(self.template.segments)

    def store_llm_text_reads(
        self, reads: Sequence[ResponseRead], llm_text_selectors: frozenset[Selector]
    ) -> None:
        for read in reads:
            if (
                read.selector not in llm_text_selectors
                or not self.index <= read.position < len(self.template.segments)
            ):
                continue
            segment = self.template.segments[read.position]
            if (
                isinstance(segment, VariableSegment)
                and tuple(segment.selector) == read.selector
            ):
                self.llm_text_reads[read.position] = read


@dataclass
class _StreamBuffers:
    """Buffered stream chunks plus per-selector read cursors."""

    events: dict[Selector, list[NodeRunStreamChunkEvent]] = field(default_factory=dict)
    positions: dict[Selector, int] = field(default_factory=dict)
    closed_selectors: set[Selector] = field(default_factory=set)
    legacy_complete_result_selectors: set[Selector] = field(default_factory=set)

    @classmethod
    def from_state(
        cls,
        *,
        buffers: Sequence[StreamBufferState],
        positions: Sequence[StreamPositionState],
        closed_selectors: Sequence[Selector],
    ) -> _StreamBuffers:
        stream_buffers = cls(
            events={
                tuple(buffer.selector): [
                    event.model_copy(deep=True) for event in buffer.events
                ]
                for buffer in buffers
            },
            positions={
                tuple(position.selector): position.position for position in positions
            },
            closed_selectors={tuple(selector) for selector in closed_selectors},
        )
        for selector in stream_buffers.events:
            stream_buffers.positions.setdefault(selector, 0)
        return stream_buffers

    def append(self, event: NodeRunStreamChunkEvent) -> None:
        key = tuple(event.selector)
        if key in self.closed_selectors:
            msg = f"Stream {'.'.join(key)} is already closed"
            raise ValueError(msg)

        if event.is_final and _is_llm_text_chunk(event):
            previous = self.events.get(key)
            if not previous:
                return
            last = previous[-1]
            if last.id != event.id or last.retry_index != event.retry_index:
                if not any(
                    chunk.chunk for chunk in islice(previous, self.positions[key])
                ):
                    self.discard(key)
                    return
                # A whole retry closes the consumed stream with an empty marker.
                event = event.model_copy(update={"chunk": ""})

        if key not in self.events:
            self.events[key] = []
            self.positions[key] = 0

        self.events[key].append(event)
        if event.is_final:
            self.closed_selectors.add(key)

    def pop(
        self,
        selector: Sequence[str],
    ) -> NodeRunStreamChunkEvent | None:
        key = tuple(selector)
        if key not in self.events:
            return None

        position = self.positions.get(key, 0)
        buffer = self.events[key]
        if position >= len(buffer):
            return None

        event = buffer[position]
        self.positions[key] = position + 1
        return event

    def has_unread(self, selector: Sequence[str]) -> bool:
        key = tuple(selector)
        if key not in self.events:
            return False

        position = self.positions.get(key, 0)
        return position < len(self.events[key])

    def has_events(self, selector: Sequence[str]) -> bool:
        return tuple(selector) in self.events

    def overrides_response_read(self, selector: Sequence[str]) -> bool:
        key = tuple(selector)
        return key not in self.legacy_complete_result_selectors and (
            key in self.events or key in self.closed_selectors
        )

    def is_closed(self, selector: Sequence[str]) -> bool:
        return tuple(selector) in self.closed_selectors

    def discard(self, selector: Selector) -> None:
        del self.events[selector]
        self.positions.pop(selector, None)
        self.closed_selectors.discard(selector)
        self.legacy_complete_result_selectors.discard(selector)

    def read_saved_llm_markers(
        self,
        execution_ids: dict[str, str],
        retry_indices: dict[str, int],
        saved_result_selectors: Sequence[Selector] | None,
    ) -> None:
        # Older writers buffered a final event even when an attempt returned whole.
        selectors = (
            self.closed_selectors
            if saved_result_selectors is None
            else saved_result_selectors
        )
        for selector in selectors:
            chunks = self.events.get(selector)
            if not chunks:
                continue
            final = chunks[-1]
            if not (
                _is_llm_text_chunk(final)
                and tuple(final.selector) == selector
                and final.frame_id == ROOT_FRAME_ID
                and final.id == execution_ids.get(final.node_id)
                and final.is_final
                and selector in self.closed_selectors
            ):
                continue
            if final.retry_index:
                retry_indices.setdefault(final.node_id, final.retry_index)
            position = self.positions.get(selector, 0)
            if (
                saved_result_selectors is not None
                or (
                    position < len(chunks)
                    and not any(chunk.chunk for chunk in islice(chunks, position))
                )
            ) and all(_is_delta_before_retry(chunk, final) for chunk in chunks[:-1]):
                # A restored reply with no recoverable read still uses this marker.
                self.legacy_complete_result_selectors.add(selector)
        if saved_result_selectors is not None and len(
            self.legacy_complete_result_selectors
        ) != len(saved_result_selectors):
            msg = "Invalid saved whole LLM stream"
            raise ValueError(msg)

    def dump_buffers(self) -> list[StreamBufferState]:
        return [
            StreamBufferState(
                selector=selector,
                events=[event.model_copy(deep=True) for event in events],
            )
            for selector, events in sorted(self.events.items())
        ]

    def dump_positions(self) -> list[StreamPositionState]:
        return [
            StreamPositionState(selector=selector, position=position)
            for selector, position in sorted(self.positions.items())
        ]

    def dump_closed_selectors(self) -> list[Selector]:
        return sorted(self.closed_selectors)


@dataclass
class _TemplateStream:
    """Source buffers and identities used to drain response templates."""

    stream_buffers: _StreamBuffers = field(default_factory=_StreamBuffers)
    node_execution_ids: dict[str, str] = field(default_factory=dict)
    node_retry_indices: dict[str, int] = field(default_factory=dict)
    llm_text_selectors: frozenset[Selector] = frozenset()
    frame_id: str = ROOT_FRAME_ID
    container_id: str = ""
    skipped_node_ids: set[str] | None = None
    source_streams: dict[tuple[str, int, Selector], _SourceStream] = field(
        default_factory=dict
    )
    latest_streams: dict[Selector, _SourceStream] = field(default_factory=dict)
    finished_node_ids: set[str] = field(default_factory=set)

    def record_attempt(self, event: NodeEvent) -> None:
        self.node_execution_ids[event.node_id] = event.id
        if event.retry_index:
            self.node_retry_indices[event.node_id] = event.retry_index
        else:
            self.node_retry_indices.pop(event.node_id, None)

    def append(self, event: NodeRunStreamChunkEvent) -> None:
        if self.frame_id == ROOT_FRAME_ID:
            self.stream_buffers.append(event)
            return
        selector = tuple(event.selector)
        key = (event.id, event.retry_index, selector)
        if key not in self.source_streams:
            if event.is_final and _is_llm_text_chunk(event):
                return
            self.source_streams[key] = _SourceStream()
            self.latest_streams[selector] = self.source_streams[key]
        source = self.source_streams[key]
        if not source.closed:
            source.events.append(event)
            source.closed = event.is_final

    def forget_replaced_streams(self, selector: Sequence[str]) -> None:
        # Existing occurrences stay pinned; later occurrences see the replacement.
        for key in list(self.latest_streams):
            if key[:2] == tuple(selector):
                del self.latest_streams[key]

    def prepare_streams_for_retry(self, event: NodeRunRetryEvent) -> None:
        if self.frame_id == ROOT_FRAME_ID:
            selector = (event.node_id, "text")
            chunks = self.stream_buffers.events.get(selector)
            if chunks and _only_empty_llm_deltas_before_retry(chunks, event):
                self.stream_buffers.discard(selector)
            return
        for (execution_id, attempt, selector), source in self.source_streams.items():
            if execution_id == event.id and attempt < event.retry_index:
                source.closed = True
                if self.latest_streams.get(selector) is source:
                    del self.latest_streams[selector]

    def finish_source(self, event: NodeEvent) -> None:
        self.finished_node_ids.add(event.node_id)
        for (execution_id, attempt, _), source in self.source_streams.items():
            if execution_id == event.id and attempt == event.retry_index:
                source.closed = True

    def finish_sources(self, node_ids: Iterable[str]) -> None:
        self.finished_node_ids.update(node_ids)
        for source in self.source_streams.values():
            source.closed = True

    def clear(self) -> None:
        self.stream_buffers = _StreamBuffers()
        self.source_streams.clear()
        self.latest_streams.clear()

    def is_skipped(self, node_id: str, *, graph: GraphProtocol) -> bool:
        if self.skipped_node_ids is not None:
            return node_id in self.skipped_node_ids
        node = graph.nodes.get(node_id)
        return bool(node and node.state == NodeState.SKIPPED)

    def finish_response(
        self, session: _ResponseSession, *, graph: GraphProtocol
    ) -> list[NodeRunStreamChunkEvent]:
        if (
            self.frame_id == ROOT_FRAME_ID
            or graph.nodes[session.node_id].node_type != BuiltinNodeTypes.ANSWER
        ):
            return []
        return [
            self._create_stream_chunk_event(
                session.node_id,
                self._get_or_create_execution_id(session.node_id),
                [session.node_id, "answer"],
                "",
                True,
                graph=graph,
            )
        ]

    def flush(
        self,
        session: _ResponseSession,
        *,
        graph: GraphProtocol,
        variable_pool: ReadOnlyVariablePool,
    ) -> list[NodeRunStreamChunkEvent]:
        events: list[NodeRunStreamChunkEvent] = []
        while session.index < len(session.template.segments):
            segment = session.template.segments[session.index]

            if isinstance(segment, VariableSegment):
                segment_events, is_complete = self._read_variable_chunks(
                    segment, session, graph=graph, variable_pool=variable_pool
                )
                events.extend(segment_events)

                if is_complete:
                    session.llm_text_reads.pop(session.index, None)
                    session.index += 1
                else:
                    break
            else:
                events.extend(self._create_text_chunks(segment, session, graph=graph))
                session.index += 1

        if (
            self.frame_id != ROOT_FRAME_ID
            and graph.nodes[session.node_id].node_type == BuiltinNodeTypes.ANSWER
        ):
            return [event for event in events if event.chunk]
        return events

    def _get_or_create_execution_id(self, node_id: str) -> str:
        if node_id not in self.node_execution_ids:
            self.node_execution_ids[node_id] = str(uuid4())
        return self.node_execution_ids[node_id]

    def _create_stream_chunk_event(
        self,
        node_id: str,
        execution_id: str,
        selector: Sequence[str],
        chunk: str,
        is_final: bool = False,
        *,
        graph: GraphProtocol,
    ) -> NodeRunStreamChunkEvent:
        node = graph.nodes[node_id]
        return NodeRunStreamChunkEvent(
            id=execution_id,
            frame_id=self.frame_id,
            container_id=self.container_id,
            node_id=node.id,
            node_type=node.node_type,
            retry_index=self.node_retry_indices.get(node_id, 0),
            selector=list(selector),
            chunk=chunk,
            is_final=is_final,
        )

    def _read_variable_chunks(
        self,
        segment: VariableSegment,
        session: _ResponseSession,
        *,
        graph: GraphProtocol,
        variable_pool: ReadOnlyVariablePool,
    ) -> tuple[list[NodeRunStreamChunkEvent], bool]:
        selector = tuple(segment.selector)
        source_id = selector[0] if selector else ""
        use_response_read = session.index in session.llm_text_reads or (
            selector in self.llm_text_selectors
            and not session.completed_without_saved_reads
            and graph.nodes[session.node_id].node_type
            in {BuiltinNodeTypes.ANSWER, BuiltinNodeTypes.END}
        )
        if not use_response_read and self.is_skipped(source_id, graph=graph):
            return [], True

        if self.frame_id == ROOT_FRAME_ID:
            has_stream = (
                self.stream_buffers.overrides_response_read(selector)
                if use_response_read
                else self.stream_buffers.has_events(selector)
                or self.stream_buffers.is_closed(selector)
            )
            if has_stream:
                return self._read_root_stream(segment, session, graph=graph)
        else:
            source = session.segment_streams.get(
                session.index
            ) or self.latest_streams.get(selector)
            if source is not None:
                return self._read_child_stream(source, session, graph=graph)

        if use_response_read:
            read = session.llm_text_reads.get(session.index)
            if read is None:
                return [], False
            value = read.value
        else:
            value = variable_pool.get(selector)
        if value is None:
            finished = source_id in self.finished_node_ids
            if use_response_read:
                # A parallel model may still send genuine tokens after this lookup.
                finished = (
                    finished
                    or source_id not in graph.nodes
                    or self.is_skipped(source_id, graph=graph)
                )
            return [], finished and (
                use_response_read or self.frame_id != ROOT_FRAME_ID
            )

        return self._emit_value(
            segment,
            session,
            value if isinstance(value, str) else value.markdown,
            graph=graph,
        )

    def _emit_value(
        self,
        segment: VariableSegment,
        session: _ResponseSession,
        text: str,
        *,
        graph: GraphProtocol,
    ) -> tuple[list[NodeRunStreamChunkEvent], bool]:
        source_id = segment.selector[0] if segment.selector else ""
        node_id = source_id if source_id in graph.nodes else session.node_id
        chunk = self._create_stream_chunk_event(
            node_id,
            self._get_or_create_execution_id(node_id),
            segment.selector,
            text,
            session.index == len(session.template.segments) - 1,
            graph=graph,
        )
        if self.frame_id == ROOT_FRAME_ID:
            return [chunk], True
        # A hidden reply can restart after a retry. Pin values it already read.
        return self._read_child_stream(
            _SourceStream([chunk], closed=True), session, graph=graph
        )

    def _read_child_stream(
        self, source: _SourceStream, session: _ResponseSession, *, graph: GraphProtocol
    ) -> tuple[list[NodeRunStreamChunkEvent], bool]:
        session.segment_streams[session.index] = source
        position = session.segment_positions.get(session.index, 0)
        events = [
            self._create_child_response_chunk(event, session, graph=graph)
            for event in source.events[position:]
        ]
        session.segment_positions[session.index] = len(source.events)
        return events, source.closed

    def _create_child_response_chunk(
        self,
        event: NodeRunStreamChunkEvent,
        session: _ResponseSession,
        *,
        graph: GraphProtocol,
    ) -> NodeRunStreamChunkEvent:
        if graph.nodes[session.node_id].node_type != BuiltinNodeTypes.ANSWER:
            return event
        return self._create_stream_chunk_event(
            session.node_id,
            self._get_or_create_execution_id(session.node_id),
            [session.node_id, "answer"],
            event.chunk,
            graph=graph,
        )

    def _read_root_stream(
        self,
        segment: VariableSegment,
        session: _ResponseSession,
        *,
        graph: GraphProtocol,
    ) -> tuple[list[NodeRunStreamChunkEvent], bool]:
        events: list[NodeRunStreamChunkEvent] = []
        while self.stream_buffers.has_unread(segment.selector):
            event = self.stream_buffers.pop(segment.selector)
            if event is None:
                continue
            if segment.selector and segment.selector[0] in graph.nodes:
                events.append(event)
            else:
                response_node = graph.nodes[session.node_id]
                events.append(
                    NodeRunStreamChunkEvent(
                        id=self._get_or_create_execution_id(response_node.id),
                        node_id=response_node.id,
                        node_type=response_node.node_type,
                        container_id=event.container_id,
                        selector=list(event.selector),
                        chunk=event.chunk,
                        is_final=event.is_final,
                    )
                )
        return events, self.stream_buffers.is_closed(segment.selector)

    def _create_text_chunks(
        self,
        segment: TextSegment,
        session: _ResponseSession,
        *,
        graph: GraphProtocol,
    ) -> list[NodeRunStreamChunkEvent]:
        current_response_node = graph.nodes[session.node_id]
        execution_id = self._get_or_create_execution_id(current_response_node.id)
        is_last_segment = session.index == len(session.template.segments) - 1
        if (
            self.frame_id != ROOT_FRAME_ID
            and current_response_node.node_type == BuiltinNodeTypes.ANSWER
        ):
            is_last_segment = False
        return [
            self._create_stream_chunk_event(
                node_id=current_response_node.id,
                execution_id=execution_id,
                selector=self._get_text_segment_selector(current_response_node),
                chunk=segment.text,
                is_final=is_last_segment,
                graph=graph,
            )
        ]

    def _get_text_segment_selector(self, response_node: NodeProtocol) -> Sequence[str]:
        get_streaming_text_selector = getattr(
            response_node,
            "get_streaming_text_selector",
            None,
        )
        if callable(get_streaming_text_selector):
            selector = get_streaming_text_selector()
            return [str(part) for part in selector]
        return [response_node.id, "answer"]
