from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, ClassVar, cast

import pytest

from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState, NodeType
from graphon.filters import (
    GraphEventFilterContext,
    ResponseStreamFilter,
    filter_graph_events,
)
from graphon.graph_events.graph import GraphRunStartedEvent
from graphon.graph_events.node import (
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
)
from graphon.graph_events.traversal import GraphEdgeTakenEvent
from graphon.nodes.base.template import (
    Template,
    TemplateSegmentUnion,
    TextSegment,
    VariableSegment,
)
from graphon.runtime.graph_runtime_state import (
    EdgeProtocol,
    GraphProtocol,
    GraphRuntimeState,
    NodeProtocol,
)
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import StringSegment


class _TestNode(NodeProtocol):
    node_type: ClassVar[NodeType] = BuiltinNodeTypes.END

    def __init__(
        self,
        node_id: str,
        *,
        execution_type: NodeExecutionType = NodeExecutionType.EXECUTABLE,
        template: Template | None = None,
    ) -> None:
        self.id = node_id
        self.execution_type = execution_type
        self.state = NodeState.UNKNOWN
        self._template = template or Template(segments=[])

    def get_streaming_template(self) -> Template:
        return self._template

    def get_streaming_text_selector(self) -> list[str]:
        return [self.id, "answer"]

    def blocks_variable_output(
        self,
        variable_selectors: set[tuple[str, ...]],
    ) -> bool:
        return bool({(self.id, "answer")} & variable_selectors)


class _BrokenResponseNode:
    node_type: ClassVar[NodeType] = BuiltinNodeTypes.END

    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.execution_type = NodeExecutionType.RESPONSE
        self.state = NodeState.UNKNOWN

    def blocks_variable_output(
        self,
        _variable_selectors: set[tuple[str, ...]],
    ) -> bool:
        return False


class _TestEdge(EdgeProtocol):
    def __init__(self, edge_id: str, tail: str, head: str) -> None:
        self.id = edge_id
        self.state = NodeState.UNKNOWN
        self.tail = tail
        self.head = head
        self.source_handle = "success"


class _TestGraph(GraphProtocol):
    def __init__(
        self,
        *,
        nodes: dict[str, _TestNode],
        edges: dict[str, _TestEdge],
        root_node_id: str,
    ) -> None:
        self._nodes = nodes
        self._edges = edges
        self._root_node = nodes[root_node_id]

    @property
    def nodes(self) -> dict[str, _TestNode]:
        return self._nodes

    @property
    def edges(self) -> dict[str, _TestEdge]:
        return self._edges

    @property
    def root_node(self) -> _TestNode:
        return self._root_node

    def get_outgoing_edges(self, node_id: str) -> Sequence[_TestEdge]:
        return [edge for edge in self.edges.values() if edge.tail == node_id]

    def get_incoming_edges(self, node_id: str) -> Sequence[_TestEdge]:
        return [edge for edge in self.edges.values() if edge.head == node_id]


def _context(
    graph: _TestGraph,
    variable_pool: VariablePool | None = None,
) -> GraphEventFilterContext:
    state = GraphRuntimeState(variable_pool=variable_pool or VariablePool(), start_at=0)
    state.configure(graph=cast(Any, graph))
    return GraphEventFilterContext(
        graph=cast(Any, graph),
        runtime_state=ReadOnlyGraphRuntimeStateWrapper(state),
    )


def _variable_response_graph(
    *,
    source_id: str = "source",
    answer_id: str = "answer",
    selector: Sequence[str] | None = None,
    segments: Sequence[TemplateSegmentUnion] | None = None,
    edge_id: str = "edge-1",
) -> _TestGraph:
    source = _TestNode(source_id)
    response_segments: list[TemplateSegmentUnion]
    if segments is None:
        response_segments = [
            VariableSegment(selector=list(selector or [source_id, "answer"])),
        ]
    else:
        response_segments = list(segments)
    answer = _TestNode(
        answer_id,
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=response_segments),
    )
    edge = _TestEdge(edge_id, source_id, answer_id)
    return _TestGraph(
        nodes={source_id: source, answer_id: answer},
        edges={edge_id: edge},
        root_node_id=source_id,
    )


def _edge_taken(
    *,
    edge_id: str = "edge-1",
    source_id: str = "source",
    answer_id: str = "answer",
) -> GraphEdgeTakenEvent:
    return GraphEdgeTakenEvent(
        edge_id=edge_id,
        source_node_id=source_id,
        target_node_id=answer_id,
        source_handle="success",
    )


def _stream_chunk(
    chunk: str,
    *,
    run_id: str = "source-run",
    source_id: str = "source",
    selector: Sequence[str] | None = None,
    is_final: bool = True,
) -> NodeRunStreamChunkEvent:
    return NodeRunStreamChunkEvent(
        id=run_id,
        node_id=source_id,
        node_type=BuiltinNodeTypes.CODE,
        selector=list(selector or [source_id, "answer"]),
        chunk=chunk,
        is_final=is_final,
    )


def test_response_stream_filter_emits_text_segments_when_session_starts() -> None:
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[TextSegment(text="Hello")]),
    )
    graph = _TestGraph(nodes={"answer": answer}, edges={}, root_node_id="answer")
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))

    output = list(event_filter.on_event(GraphRunStartedEvent()))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [chunk.chunk for chunk in chunks] == ["Hello"]
    assert chunks[0].selector == ["answer", "answer"]
    assert chunks[0].is_final is True


def test_response_stream_filter_rejects_events_before_initialize() -> None:
    event_filter = ResponseStreamFilter()

    with pytest.raises(RuntimeError, match="initialized"):
        list(event_filter.on_event(GraphRunStartedEvent()))

    with pytest.raises(RuntimeError, match="initialized"):
        list(event_filter.flush())

    with pytest.raises(RuntimeError, match="initialized"):
        event_filter.dumps()


def test_response_stream_filter_resets_when_initialize_fails() -> None:
    graph = _TestGraph(
        nodes=cast(Any, {"answer": _BrokenResponseNode("answer")}),
        edges={},
        root_node_id="answer",
    )
    event_filter = ResponseStreamFilter()

    with pytest.raises(TypeError, match="get_streaming_template"):
        event_filter.initialize(_context(graph))

    with pytest.raises(RuntimeError, match="initialized"):
        list(event_filter.on_event(GraphRunStartedEvent()))


def test_response_stream_filter_reorders_buffered_stream_chunks_after_edge_taken() -> (
    None
):
    graph = _variable_response_graph(
        segments=[
            TextSegment(text="prefix "),
            VariableSegment(selector=["source", "answer"]),
        ],
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))

    started = NodeRunStartedEvent(
        id="source-run",
        node_id="source",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Source",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    chunk = _stream_chunk("value")
    taken = _edge_taken()

    assert list(event_filter.on_event(started)) == [started]
    assert list(event_filter.on_event(chunk)) == []
    output = list(event_filter.on_event(taken))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["prefix ", "value"]


def test_response_stream_filter_reads_scalar_variable_values() -> None:
    graph = _variable_response_graph()
    variable_pool = VariablePool()
    variable_pool.add(["source", "answer"], StringSegment(value="saved"))
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph, variable_pool))

    output = list(event_filter.on_event(_edge_taken()))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["saved"]


def test_response_stream_filter_uses_retry_execution_id_for_scalar_value() -> None:
    graph = _variable_response_graph()
    variable_pool = VariablePool()
    variable_pool.add(["source", "answer"], StringSegment(value="saved"))
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph, variable_pool))
    retry = NodeRunRetryEvent(
        id="retry-run",
        node_id="source",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Source",
        start_at=datetime.now(UTC).replace(tzinfo=None),
        error="temporary",
        retry_index=1,
    )

    assert list(event_filter.on_event(retry)) == [retry]
    output = list(event_filter.on_event(_edge_taken()))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [(event.id, event.chunk) for event in chunks] == [("retry-run", "saved")]


def test_response_stream_filter_initialize_resets_run_state() -> None:
    graph = _variable_response_graph()
    context = _context(graph)
    event_filter = ResponseStreamFilter()
    event_filter.initialize(context)
    first_chunk = _stream_chunk("first", run_id="source-run-1")
    assert list(event_filter.on_event(first_chunk)) == []
    first_output = list(event_filter.on_event(_edge_taken()))
    first_chunks = [
        event for event in first_output if isinstance(event, NodeRunStreamChunkEvent)
    ]
    assert [event.chunk for event in first_chunks] == ["first"]

    event_filter.initialize(context)
    second_chunk = _stream_chunk("second", run_id="source-run-2")
    assert list(event_filter.on_event(second_chunk)) == []
    second_output = list(event_filter.on_event(_edge_taken()))

    second_chunks = [
        event for event in second_output if isinstance(event, NodeRunStreamChunkEvent)
    ]
    assert [event.chunk for event in second_chunks] == ["second"]


def test_response_stream_filter_round_trips_resume_state() -> None:
    graph = _variable_response_graph()
    context = _context(graph)
    first_filter = ResponseStreamFilter()
    first_filter.initialize(context)
    raw_chunk = _stream_chunk("resumed")
    assert list(first_filter.on_event(raw_chunk)) == []

    restored_filter = ResponseStreamFilter()
    restored_filter.initialize(context)
    restored_filter.loads(first_filter.dumps())
    output = list(restored_filter.on_event(_edge_taken()))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["resumed"]


def test_response_stream_filter_can_load_before_filter_chain_initializes() -> None:
    graph = _variable_response_graph()
    context = _context(graph)
    first_filter = ResponseStreamFilter()
    first_filter.initialize(context)
    raw_chunk = _stream_chunk("chain-resumed")
    assert list(first_filter.on_event(raw_chunk)) == []

    restored_filter = ResponseStreamFilter()
    snapshot = first_filter.dumps()
    restored_filter.loads(snapshot)
    assert restored_filter.dumps() == snapshot
    output = list(
        filter_graph_events(
            [_edge_taken()],
            context=context,
            filters=[restored_filter],
        )
    )

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["chain-resumed"]


def test_response_stream_filter_restores_referenced_selectors() -> None:
    graph = _variable_response_graph()
    context = _context(graph)
    first_filter = ResponseStreamFilter()
    first_filter.initialize(context)

    restored_filter = ResponseStreamFilter()
    restored_filter.loads(first_filter.dumps())
    output = list(
        filter_graph_events(
            [
                _edge_taken(),
                _stream_chunk("late"),
            ],
            context=context,
            filters=[restored_filter],
        )
    )

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["late"]


def test_response_stream_filter_keeps_pending_state_when_initialize_fails() -> None:
    graph = _variable_response_graph()
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    snapshot = event_filter.dumps()

    incompatible_graph = _TestGraph(
        nodes={"source": _TestNode("source")},
        edges={},
        root_node_id="source",
    )
    restored_filter = ResponseStreamFilter()
    restored_filter.loads(snapshot)

    with pytest.raises(ValueError, match="Unknown response node 'answer'"):
        restored_filter.initialize(_context(incompatible_graph))

    assert restored_filter.dumps() == snapshot
    restored_filter.loads(snapshot)
    assert restored_filter.dumps() == snapshot
    with pytest.raises(RuntimeError, match="initialized"):
        list(restored_filter.on_event(GraphRunStartedEvent()))


def test_response_stream_filter_keeps_current_state_when_load_fails() -> None:
    graph = _variable_response_graph()
    context = _context(graph)
    event_filter = ResponseStreamFilter()
    event_filter.initialize(context)
    raw_chunk = _stream_chunk("preserved")
    assert list(event_filter.on_event(raw_chunk)) == []

    other_graph = _variable_response_graph(
        source_id="other-source",
        answer_id="other-answer",
        edge_id="other-edge",
    )
    other_filter = ResponseStreamFilter()
    other_filter.initialize(_context(other_graph))

    with pytest.raises(ValueError, match="Unknown response node 'other-answer'"):
        event_filter.loads(other_filter.dumps())

    output = list(event_filter.on_event(_edge_taken()))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["preserved"]
