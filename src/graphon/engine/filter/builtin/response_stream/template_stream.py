"""Template streaming for ResponseStreamFilter's root Answer and End sessions.

The filter owns this state and selects which session drains. Separate instances
can support later child-frame sessions; child streaming is not implemented here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from uuid import uuid4

from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.enums import NodeState
from graphon.nodes.base.template import Template, TextSegment, VariableSegment
from graphon.runtime.runtime_state import GraphProtocol, NodeProtocol
from graphon.runtime.runtime_state_protocol import ReadOnlyVariablePool

from .snapshot import Selector, StreamBufferState, StreamPositionState


@dataclass
class _ResponseSession:
    """Streaming cursor for one response node template."""

    node_id: str
    template: Template
    index: int = 0

    @classmethod
    def from_node(cls, node: NodeProtocol) -> _ResponseSession:
        get_streaming_template = getattr(node, "get_streaming_template", None)
        if not callable(get_streaming_template):
            msg = (
                "Response streaming requires get_streaming_template() on response nodes"
            )
            raise TypeError(msg)
        return cls(node_id=node.id, template=get_streaming_template())

    def is_complete(self) -> bool:
        return self.index >= len(self.template.segments)


@dataclass
class _StreamBuffers:
    """Buffered stream chunks plus per-selector read cursors."""

    events: dict[Selector, list[NodeRunStreamChunkEvent]] = field(default_factory=dict)
    positions: dict[Selector, int] = field(default_factory=dict)
    closed_selectors: set[Selector] = field(default_factory=set)

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

    def append(
        self,
        selector: Sequence[str],
        event: NodeRunStreamChunkEvent,
    ) -> None:
        key = tuple(selector)
        if key in self.closed_selectors:
            msg = f"Stream {'.'.join(selector)} is already closed"
            raise ValueError(msg)

        if key not in self.events:
            self.events[key] = []
            self.positions[key] = 0

        self.events[key].append(event)

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

    def close(self, selector: Sequence[str]) -> None:
        self.closed_selectors.add(tuple(selector))

    def is_closed(self, selector: Sequence[str]) -> bool:
        return tuple(selector) in self.closed_selectors

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
                source_selector_prefix = segment.selector[0] if segment.selector else ""
                if source_selector_prefix in graph.nodes:
                    source_node = graph.nodes[source_selector_prefix]
                    if source_node.state == NodeState.SKIPPED:
                        session.index += 1
                        continue

                segment_events, is_complete = self._read_variable_chunks(
                    segment, session, graph=graph, variable_pool=variable_pool
                )
                events.extend(segment_events)

                if is_complete:
                    session.index += 1
                else:
                    break
            else:
                events.extend(self._create_text_chunks(segment, session, graph=graph))
                session.index += 1

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
            node_id=node.id,
            node_type=node.node_type,
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
        events: list[NodeRunStreamChunkEvent] = []
        source_selector_prefix = segment.selector[0] if segment.selector else ""
        is_complete = False

        is_non_node_selector = source_selector_prefix not in graph.nodes
        if is_non_node_selector:
            output_node_id = session.node_id
        else:
            output_node_id = source_selector_prefix
        execution_id = self._get_or_create_execution_id(output_node_id)
        has_stream_events = self.stream_buffers.has_events(segment.selector)

        while self.stream_buffers.has_unread(segment.selector):
            event = self.stream_buffers.pop(segment.selector)
            if event is None:
                continue

            if is_non_node_selector:
                response_node = graph.nodes[session.node_id]
                events.append(
                    NodeRunStreamChunkEvent(
                        id=execution_id,
                        node_id=response_node.id,
                        node_type=response_node.node_type,
                        container_id=event.container_id,
                        selector=list(event.selector),
                        chunk=event.chunk,
                        is_final=event.is_final,
                    )
                )
            else:
                events.append(event)

        if self.stream_buffers.is_closed(segment.selector):
            is_complete = True
        elif not has_stream_events and (value := variable_pool.get(segment.selector)):
            is_last_segment = session.index == len(session.template.segments) - 1
            events.append(
                self._create_stream_chunk_event(
                    node_id=output_node_id,
                    execution_id=execution_id,
                    selector=segment.selector,
                    chunk=value.markdown,
                    is_final=is_last_segment,
                    graph=graph,
                ),
            )
            is_complete = True

        return events, is_complete

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
