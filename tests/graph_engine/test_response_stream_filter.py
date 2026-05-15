from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, ClassVar, cast

from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState, NodeType
from graphon.graph_engine.filters import GraphEventFilterContext, ResponseStreamFilter
from graphon.graph_events.graph import GraphRunStartedEvent
from graphon.graph_events.node import NodeRunStartedEvent, NodeRunStreamChunkEvent
from graphon.graph_events.traversal import GraphEdgeTakenEvent
from graphon.nodes.base.template import Template, TextSegment, VariableSegment
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


class _TestEdge(EdgeProtocol):
    def __init__(self, edge_id: str, tail: str, head: str) -> None:
        self.id = edge_id
        self.state = NodeState.UNKNOWN
        self.tail = tail
        self.head = head
        self.source_handle = "success"


class _TestGraph(GraphProtocol):
    nodes: dict[str, _TestNode]
    edges: dict[str, _TestEdge]
    root_node: _TestNode

    def __init__(
        self,
        *,
        nodes: dict[str, _TestNode],
        edges: dict[str, _TestEdge],
        root_node_id: str,
    ) -> None:
        self.nodes = nodes
        self.edges = edges
        self.root_node = nodes[root_node_id]

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


def test_response_stream_filter_reorders_buffered_stream_chunks_after_edge_taken() -> (
    None
):
    source = _TestNode("source")
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(
            segments=[
                TextSegment(text="prefix "),
                VariableSegment(selector=["source", "answer"]),
            ]
        ),
    )
    edge = _TestEdge("edge-1", "source", "answer")
    graph = _TestGraph(
        nodes={"source": source, "answer": answer},
        edges={"edge-1": edge},
        root_node_id="source",
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
    chunk = NodeRunStreamChunkEvent(
        id="source-run",
        node_id="source",
        node_type=BuiltinNodeTypes.CODE,
        selector=["source", "answer"],
        chunk="value",
        is_final=True,
    )
    taken = GraphEdgeTakenEvent(
        edge_id="edge-1",
        source_node_id="source",
        target_node_id="answer",
        source_handle="success",
    )

    assert list(event_filter.on_event(started)) == [started]
    assert list(event_filter.on_event(chunk)) == []
    output = list(event_filter.on_event(taken))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["prefix ", "value"]


def test_response_stream_filter_reads_scalar_variable_values() -> None:
    source = _TestNode("source")
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["source", "answer"])]),
    )
    edge = _TestEdge("edge-1", "source", "answer")
    graph = _TestGraph(
        nodes={"source": source, "answer": answer},
        edges={"edge-1": edge},
        root_node_id="source",
    )
    variable_pool = VariablePool()
    variable_pool.add(["source", "answer"], StringSegment(value="saved"))
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph, variable_pool))

    output = list(
        event_filter.on_event(
            GraphEdgeTakenEvent(
                edge_id="edge-1",
                source_node_id="source",
                target_node_id="answer",
                source_handle="success",
            )
        )
    )

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["saved"]


def test_response_stream_filter_round_trips_resume_state() -> None:
    source = _TestNode("source")
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["source", "answer"])]),
    )
    edge = _TestEdge("edge-1", "source", "answer")
    graph = _TestGraph(
        nodes={"source": source, "answer": answer},
        edges={"edge-1": edge},
        root_node_id="source",
    )
    context = _context(graph)
    first_filter = ResponseStreamFilter()
    first_filter.initialize(context)
    raw_chunk = NodeRunStreamChunkEvent(
        id="source-run",
        node_id="source",
        node_type=BuiltinNodeTypes.CODE,
        selector=["source", "answer"],
        chunk="resumed",
        is_final=True,
    )
    assert list(first_filter.on_event(raw_chunk)) == []

    restored_filter = ResponseStreamFilter()
    restored_filter.initialize(context)
    restored_filter.loads(first_filter.dumps())
    output = list(
        restored_filter.on_event(
            GraphEdgeTakenEvent(
                edge_id="edge-1",
                source_node_id="source",
                target_node_id="answer",
                source_handle="success",
            )
        )
    )

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["resumed"]
