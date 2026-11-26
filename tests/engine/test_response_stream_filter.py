import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.node import (
    NodeRunReasoningChunkEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
)
from graphon.engine_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState, NodeType
from graphon.graph.graph import Graph
from graphon.nodes.base.template import (
    Template,
    TemplateSegmentUnion,
    TextSegment,
    VariableSegment,
)
from graphon.runtime.execution import ROOT_FRAME_ID
from graphon.runtime.read_only_wrappers import ReadOnlyRuntimeStateWrapper
from graphon.runtime.runtime_state import (
    EdgeProtocol,
    GraphProtocol,
    NodeProtocol,
    RuntimeState,
)
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import StringSegment

_V1_FIXTURE_DIR = Path(__file__).with_name("fixtures")
# These snapshots are the exact output of ResponseStreamFilter.dumps() at c3870df.
_V1_SIMPLE_SNAPSHOT = (
    _V1_FIXTURE_DIR / "response_stream_filter_v1_c3870df_simple.json"
).read_text()
_V1_NESTED_SNAPSHOT = (
    _V1_FIXTURE_DIR / "response_stream_filter_v1_c3870df_nested.json"
).read_text()


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
        graph_config: Mapping[str, Any] | None = None,
    ) -> None:
        self._nodes = nodes
        self._edges = edges
        self._root_node = nodes[root_node_id]
        self._graph_config: Mapping[str, Any] | None = graph_config or {
            "edges": [
                {
                    "id": edge.id,
                    "source": edge.tail,
                    "target": edge.head,
                }
                for edge in edges.values()
            ]
        }

    @property
    def nodes(self) -> dict[str, _TestNode]:
        return self._nodes

    @property
    def edges(self) -> dict[str, _TestEdge]:
        return self._edges

    @property
    def graph_config(self) -> Mapping[str, Any] | None:
        """Return the edge definition used by snapshot compatibility tests."""
        return self._graph_config

    @graph_config.setter
    def graph_config(self, value: Mapping[str, Any] | None) -> None:
        """Replace the edge definition for a compatibility test scenario."""
        self._graph_config = value

    @property
    def root_node(self) -> _TestNode:
        return self._root_node

    def get_outgoing_edges(self, node_id: str) -> Sequence[_TestEdge]:
        return [edge for edge in self.edges.values() if edge.tail == node_id]

    def get_incoming_edges(self, node_id: str) -> Sequence[_TestEdge]:
        return [edge for edge in self.edges.values() if edge.head == node_id]


class _EdgeReadLimitGraph(_TestGraph):
    def __init__(self, graph: _TestGraph) -> None:
        super().__init__(
            nodes=graph.nodes,
            edges=graph.edges,
            root_node_id=graph.root_node.id,
        )
        self._outgoing_edge_reads = 0
        self._edge_read_limit = 8 * (len(self.nodes) + len(self.edges))

    def get_outgoing_edges(self, node_id: str) -> Sequence[_TestEdge]:
        self._outgoing_edge_reads += 1
        assert self._outgoing_edge_reads <= self._edge_read_limit, (
            "Response initialization exceeded the linear graph traversal budget"
        )
        return super().get_outgoing_edges(node_id)


def _context(
    graph: GraphProtocol,
    variable_pool: VariablePool | None = None,
) -> EngineEventFilterContext:
    state = RuntimeState(
        workflow_id="workflow",
        variable_pool=variable_pool or VariablePool(),
        start_at=0,
    )
    state.attach_graph(cast(Any, graph))
    return EngineEventFilterContext(
        graph=cast(Any, graph),
        runtime_state=ReadOnlyRuntimeStateWrapper(state),
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
    frame_id: str = ROOT_FRAME_ID,
) -> GraphEdgeTakenEvent:
    return GraphEdgeTakenEvent(
        frame_id=frame_id,
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


def _reasoning_chunk(
    chunk: str,
    *,
    run_id: str = "source-run",
    source_id: str = "source",
    selector: Sequence[str] | None = None,
    is_final: bool = False,
) -> NodeRunReasoningChunkEvent:
    return NodeRunReasoningChunkEvent(
        id=run_id,
        node_id=source_id,
        node_type=BuiltinNodeTypes.CODE,
        selector=list(selector or [source_id, "reasoning_content"]),
        chunk=chunk,
        is_final=is_final,
    )


def test_response_stream_filter_handles_exponential_paths_with_bounded_work() -> None:
    stage_count = 12
    nodes = {
        "start": _TestNode("start", execution_type=NodeExecutionType.ROOT),
        "answer": _TestNode(
            "answer",
            execution_type=NodeExecutionType.RESPONSE,
            template=Template(segments=[TextSegment(text="done")]),
        ),
    }
    edges = {"start-gate": _TestEdge("start-gate", "start", "gate-0")}
    for index in range(stage_count):
        node_id = f"gate-{index}"
        nodes[node_id] = _TestNode(node_id, execution_type=NodeExecutionType.BRANCH)
        target_node_id = f"gate-{index + 1}" if index + 1 < stage_count else "answer"
        for handle in ("true", "false"):
            edge_id = f"{node_id}-{handle}"
            edge = _TestEdge(edge_id, node_id, target_node_id)
            edge.source_handle = handle
            edges[edge_id] = edge
    # Both handles converge at every stage: 2**12 paths in a 14-node DAG.
    graph = _EdgeReadLimitGraph(
        _TestGraph(nodes=nodes, edges=edges, root_node_id="start")
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    snapshot_size_limit = 1024 * (len(nodes) + len(edges))
    assert len(event_filter.dumps()) <= snapshot_size_limit
    started = GraphRunStartedEvent()
    assert list(event_filter.on_event(started)) == [started]

    for index in range(stage_count // 2):
        edge = edges[f"gate-{index}-true"]
        assert (
            list(
                event_filter.on_event(
                    _edge_taken(
                        edge_id=edge.id, source_id=edge.tail, answer_id=edge.head
                    )
                )
            )
            == []
        )
    saved_state = event_filter.dumps()
    assert len(saved_state) <= snapshot_size_limit
    restored_filter = ResponseStreamFilter()
    restored_filter.loads(saved_state)
    restored_filter.initialize(_context(graph))

    for index in range(stage_count // 2, stage_count):
        edge = edges[f"gate-{index}-true"]
        output = list(
            restored_filter.on_event(
                _edge_taken(edge_id=edge.id, source_id=edge.tail, answer_id=edge.head)
            )
        )
        if index + 1 < stage_count:
            assert output == []
        else:
            chunks = [
                event for event in output if isinstance(event, NodeRunStreamChunkEvent)
            ]
            assert [event.chunk for event in chunks] == ["done"]


@pytest.mark.parametrize("downstream_first", [False, True])
def test_response_stream_filter_waits_for_every_blocking_edge_on_one_path(
    downstream_first: bool,
) -> None:
    graph = _TestGraph(
        nodes={
            "start": _TestNode("start", execution_type=NodeExecutionType.BRANCH),
            "branch": _TestNode("branch", execution_type=NodeExecutionType.BRANCH),
            "answer": _TestNode(
                "answer",
                execution_type=NodeExecutionType.RESPONSE,
                template=Template(segments=[TextSegment(text="ready")]),
            ),
        },
        edges={
            "upstream": _TestEdge("upstream", "start", "branch"),
            "downstream": _TestEdge("downstream", "branch", "answer"),
        },
        root_node_id="start",
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    list(event_filter.on_event(GraphRunStartedEvent()))
    edge_ids = (
        ["downstream", "upstream"] if downstream_first else ["upstream", "downstream"]
    )
    first, second = (graph.edges[edge_id] for edge_id in edge_ids)
    first_event = _edge_taken(
        edge_id=first.id, source_id=first.tail, answer_id=first.head
    )
    second_event = _edge_taken(
        edge_id=second.id, source_id=second.tail, answer_id=second.head
    )

    assert list(event_filter.on_event(first_event)) == []
    assert list(event_filter.on_event(first_event)) == []
    output = list(event_filter.on_event(second_event))
    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["ready"]
    assert list(event_filter.on_event(second_event)) == []


def test_response_stream_filter_blocks_each_response_for_its_own_selectors() -> None:
    source = _TestNode("source")
    first = _TestNode(
        "first",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["source", "text"])]),
    )
    second = _TestNode(
        "second",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["source", "answer"])]),
    )
    graph = _TestGraph(
        nodes={node.id: node for node in (source, first, second)},
        edges={
            "first-edge": _TestEdge("first-edge", source.id, first.id),
            "second-edge": _TestEdge("second-edge", source.id, second.id),
        },
        root_node_id=source.id,
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    list(event_filter.on_event(GraphRunStartedEvent()))
    blocked_chunk = _stream_chunk("blocked")
    streaming_chunk = _stream_chunk("streaming", selector=["source", "text"])

    assert list(event_filter.on_event(blocked_chunk)) == []
    assert list(event_filter.on_event(streaming_chunk)) == [streaming_chunk]
    output = list(
        event_filter.on_event(_edge_taken(edge_id="second-edge", answer_id=second.id))
    )
    assert output == [blocked_chunk]


def test_response_stream_filter_passes_answer_visible_reasoning_chunk() -> None:
    graph = _variable_response_graph(selector=["source", "text"])
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk("thinking...")

    list(event_filter.on_event(GraphRunStartedEvent()))
    output = list(event_filter.on_event(reasoning))

    assert output == [reasoning]
    assert output[0] is reasoning
    assert not event_filter._stream_buffers.has_events(reasoning.selector)


def test_response_stream_filter_passes_reasoning_for_text_selector_prefix() -> None:
    graph = _variable_response_graph(selector=["source", "text", "summary"])
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk("thinking...")

    list(event_filter.on_event(GraphRunStartedEvent()))
    output = list(event_filter.on_event(reasoning))

    assert output == [reasoning]
    assert output[0] is reasoning
    assert not event_filter._stream_buffers.has_events(reasoning.selector)


def test_response_stream_filter_passes_explicit_reasoning_reference() -> None:
    graph = _variable_response_graph(selector=["source", "reasoning_content"])
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk("thinking...")

    list(event_filter.on_event(GraphRunStartedEvent()))
    output = list(event_filter.on_event(reasoning))

    assert output == [reasoning]
    assert output[0] is reasoning
    assert not event_filter._stream_buffers.has_events(reasoning.selector)


def test_response_stream_filter_filters_unreferenced_reasoning_chunk() -> None:
    graph = _variable_response_graph()
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))

    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(_reasoning_chunk("thinking..."))) == []


def test_response_stream_filter_filters_unmatched_reasoning_when_enabled() -> None:
    graph = _variable_response_graph()
    event_filter = ResponseStreamFilter(pass_unmatched_chunks=True)
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk("thinking...")

    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(reasoning)) == []


def test_response_stream_filter_passes_visible_final_reasoning_marker() -> None:
    graph = _variable_response_graph(selector=["source", "text"])
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    marker = _reasoning_chunk("", is_final=True)

    list(event_filter.on_event(GraphRunStartedEvent()))
    output = list(event_filter.on_event(marker))

    assert output == [marker]
    assert marker.is_final is True
    assert not event_filter._stream_buffers.has_events(marker.selector)


def test_response_stream_filter_filters_reasoning_before_branch_is_reached() -> None:
    branch = _TestNode("branch", execution_type=NodeExecutionType.BRANCH)
    source = _TestNode("source")
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["source", "text"])]),
    )
    graph = _TestGraph(
        nodes={"branch": branch, "source": source, "answer": answer},
        edges={
            "branch-source": _TestEdge("branch-source", "branch", "source"),
            "source-answer": _TestEdge("source-answer", "source", "answer"),
        },
        root_node_id="branch",
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk("thinking...")

    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(reasoning)) == []
    edge_output = list(
        event_filter.on_event(
            GraphEdgeTakenEvent(
                frame_id=ROOT_FRAME_ID,
                edge_id="branch-source",
                source_node_id="branch",
                target_node_id="source",
                source_handle="success",
            )
        )
    )
    assert edge_output == []
    assert list(event_filter.on_event(reasoning)) == [reasoning]


def test_response_stream_filter_filters_reasoning_from_skipped_merge_branch() -> None:
    branch = _TestNode("branch", execution_type=NodeExecutionType.BRANCH)
    chosen = _TestNode("chosen")
    skipped = _TestNode("skipped")
    skipped.state = NodeState.SKIPPED
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(
            segments=[
                VariableSegment(selector=["chosen", "text"]),
                VariableSegment(selector=["skipped", "text"]),
            ],
        ),
    )
    graph = _TestGraph(
        nodes={
            "branch": branch,
            "chosen": chosen,
            "skipped": skipped,
            "answer": answer,
        },
        edges={
            "branch-chosen": _TestEdge("branch-chosen", "branch", "chosen"),
            "chosen-answer": _TestEdge("chosen-answer", "chosen", "answer"),
            "branch-skipped": _TestEdge("branch-skipped", "branch", "skipped"),
            "skipped-answer": _TestEdge("skipped-answer", "skipped", "answer"),
        },
        root_node_id="branch",
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    chosen_reasoning = _reasoning_chunk("selected", source_id="chosen")
    skipped_reasoning = _reasoning_chunk("hidden", source_id="skipped")

    list(event_filter.on_event(GraphRunStartedEvent()))
    list(
        event_filter.on_event(
            GraphEdgeTakenEvent(
                frame_id=ROOT_FRAME_ID,
                edge_id="branch-chosen",
                source_node_id="branch",
                target_node_id="chosen",
                source_handle="success",
            )
        )
    )

    assert list(event_filter.on_event(chosen_reasoning)) == [chosen_reasoning]
    assert list(event_filter.on_event(skipped_reasoning)) == []


def test_response_stream_filter_passes_reasoning_for_waiting_session() -> None:
    source = _TestNode("source")
    active_answer = _TestNode(
        "active-answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["missing", "value"])]),
    )
    waiting_answer = _TestNode(
        "waiting-answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["source", "text"])]),
    )
    graph = _TestGraph(
        nodes={
            "source": source,
            "active-answer": active_answer,
            "waiting-answer": waiting_answer,
        },
        edges={
            "source-active": _TestEdge("source-active", "source", "active-answer"),
            "source-waiting": _TestEdge("source-waiting", "source", "waiting-answer"),
        },
        root_node_id="source",
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk("waiting")

    list(event_filter.on_event(GraphRunStartedEvent()))

    assert list(event_filter.on_event(reasoning)) == [reasoning]


def test_response_stream_filter_filters_mismatched_reasoning_selector() -> None:
    graph = _variable_response_graph(
        source_id="visible",
        selector=["visible", "reasoning_content"],
    )
    event_filter = ResponseStreamFilter(pass_unmatched_chunks=True)
    event_filter.initialize(_context(graph))
    reasoning = _reasoning_chunk(
        "borrowed",
        source_id="hidden",
        selector=["visible", "reasoning_content"],
    )

    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(reasoning)) == []


def test_response_stream_filter_restores_reasoning_visibility() -> None:
    graph = _variable_response_graph(selector=["source", "text"])
    context = _context(graph)
    first_filter = ResponseStreamFilter()
    first_filter.initialize(context)
    list(first_filter.on_event(GraphRunStartedEvent()))

    restored_filter = ResponseStreamFilter()
    restored_filter.initialize(context)
    restored_filter.loads(first_filter.dumps())
    reasoning = _reasoning_chunk("resumed")

    assert list(restored_filter.on_event(reasoning)) == [reasoning]


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


def test_response_stream_filter_ignores_child_frame_edge_id_collision() -> None:
    """Verify that a child traversal event cannot unblock a root response path.

    The child event deliberately reuses the root edge ID and remains filtered;
    only the matching root event may release the buffered response chunk.
    """
    graph = _variable_response_graph(edge_id="edge-1")
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    chunk = _stream_chunk("root response")

    assert list(event_filter.on_event(chunk)) == []
    assert list(event_filter.on_event(_edge_taken(frame_id="loop-frame"))) == []

    output = list(event_filter.on_event(_edge_taken()))
    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["root response"]


@pytest.mark.parametrize("edge_taken_before_initialize", [False, True])
def test_response_stream_filter_waits_for_consumed_edge_event(
    edge_taken_before_initialize: bool,
) -> None:
    graph = _variable_response_graph()
    event_filter = ResponseStreamFilter()
    if edge_taken_before_initialize:
        graph.edges["edge-1"].state = NodeState.TAKEN
    event_filter.initialize(_context(graph))
    if not edge_taken_before_initialize:
        graph.edges["edge-1"].state = NodeState.TAKEN
    chunk = _stream_chunk("buffered")

    # The engine can advance while its events are still waiting for the consumer.
    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(chunk)) == []

    # The 0.8 filter ignores skip notifications, including before a later take.
    skipped = GraphEdgeSkippedEvent(
        frame_id=ROOT_FRAME_ID,
        edge_id="edge-1",
        source_node_id="source",
        target_node_id="answer",
    )
    assert list(event_filter.on_event(skipped)) == []
    output = list(event_filter.on_event(_edge_taken()))

    assert output == [chunk]


def test_response_stream_filter_reads_scalar_variable_values() -> None:
    graph = _variable_response_graph()
    variable_pool = VariablePool()
    variable_pool.add(["source", "answer"], StringSegment(value="saved"))
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph, variable_pool))

    output = list(event_filter.on_event(_edge_taken()))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["saved"]


def test_response_stream_filter_does_not_mix_buffered_chunks_with_final_value() -> None:
    graph = _variable_response_graph()
    variable_pool = VariablePool()
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph, variable_pool))

    assert list(event_filter.on_event(_stream_chunk("Quiet", is_final=False))) == []
    variable_pool.add(["source", "answer"], StringSegment(value="Quiet Night Thought"))

    output = [
        *event_filter.on_event(_edge_taken()),
        *event_filter.on_event(_stream_chunk(" Night", is_final=False)),
        *event_filter.on_event(_stream_chunk(" Thought")),
    ]

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["Quiet", " Night", " Thought"]


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


def test_response_stream_filter_writes_version_3_snapshot() -> None:
    graph = _variable_response_graph()
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))

    snapshot = json.loads(event_filter.dumps())

    assert snapshot["version"] == "3.0"


def test_response_stream_filter_migrates_v1_root_state_after_graph_binding() -> None:
    graph = _variable_response_graph(edge_id="edge_0")
    graph.graph_config = {
        "edges": [
            {
                "id": "edge_0",
                "source": "child-source",
                "target": "child-answer",
            },
            {
                "id": "edge_0",
                "source": "source",
                "target": "answer",
            },
        ]
    }
    restored_filter = ResponseStreamFilter()
    restored_filter.loads(_V1_NESTED_SNAPSHOT)
    with pytest.raises(RuntimeError, match="must be initialized"):
        restored_filter.dumps()
    restored_filter.initialize(_context(graph))

    assert json.loads(restored_filter.dumps()) == {
        "type": "ResponseStreamFilter",
        "version": "3.0",
        "response_nodes": ["answer"],
        "active_session": None,
        "waiting_sessions": [],
        "pending_sessions": [{"node_id": "answer", "index": 0}],
        "node_execution_ids": {},
        "legacy_paths_map": {"answer": [["edge_0"]]},
        "taken_edges": [],
        "stream_buffers": [],
        "stream_positions": [],
        "closed_streams": [],
    }

    assert list(restored_filter.on_event(_stream_chunk("value"))) == []
    output = list(restored_filter.on_event(_edge_taken(edge_id="edge_0")))
    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["value"]


def test_response_stream_filter_migrates_v1_graph_builder_state() -> None:
    source = _TestNode("source", execution_type=NodeExecutionType.ROOT)
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(
            segments=[VariableSegment(selector=["source", "answer"])],
        ),
    )
    graph = Graph.new().add_root(cast(Any, source)).add_node(cast(Any, answer)).build()
    assert graph.graph_config is None

    restored_filter = ResponseStreamFilter()
    restored_filter.loads(_V1_SIMPLE_SNAPSHOT)
    restored_filter.initialize(_context(graph))

    assert json.loads(restored_filter.dumps())["legacy_paths_map"] == {
        "answer": [["edge_0"]]
    }

    assert list(restored_filter.on_event(_stream_chunk("value"))) == []
    output = list(restored_filter.on_event(_edge_taken(edge_id="edge_0")))
    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["value"]


def test_response_stream_filter_rejects_v1_graph_without_stable_legacy_ids() -> None:
    graph = _variable_response_graph(edge_id="public-edge")
    graph.graph_config = None

    restored_filter = ResponseStreamFilter()
    restored_filter.loads(_V1_SIMPLE_SNAPSHOT)

    with pytest.raises(ValueError, match="requires stable edge_N IDs"):
        restored_filter.initialize(_context(graph))


def test_response_stream_filter_does_not_migrate_version_2_edge_ids() -> None:
    graph = _variable_response_graph(edge_id="public-edge")
    snapshot = {
        "type": "ResponseStreamFilter",
        "version": "2.0",
        "response_nodes": ["answer"],
        "pending_sessions": [{"node_id": "answer", "index": 0}],
        "paths_map": {"answer": [["edge_0"]]},
    }

    restored_filter = ResponseStreamFilter()
    restored_filter.initialize(_context(graph))
    restored_filter.loads(json.dumps(snapshot))
    assert list(restored_filter.on_event(_stream_chunk("value"))) == []

    assert list(restored_filter.on_event(_edge_taken(edge_id="public-edge"))) == []
    output = list(restored_filter.on_event(_edge_taken(edge_id="edge_0")))

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["value"]


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


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_response_stream_filter_restores_partly_consumed_legacy_path(
    version: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response_graph = _variable_response_graph(edge_id="edge_1")
    graph = _TestGraph(
        nodes={
            "branch": _TestNode("branch", execution_type=NodeExecutionType.BRANCH),
            **response_graph.nodes,
        },
        edges={
            "edge_0": _TestEdge("edge_0", "branch", "source"),
            **response_graph.edges,
        },
        root_node_id="branch",
    )
    for edge in graph.edges.values():
        edge.state = NodeState.TAKEN
    # The filter consumed edge_0 before pausing, but not the engine's edge_1 event.
    legacy_snapshot = json.dumps({
        "type": "ResponseStreamFilter",
        "version": version,
        "response_nodes": ["answer"],
        "active_session": None,
        "waiting_sessions": [],
        "pending_sessions": [{"node_id": "answer", "index": 0}],
        "node_execution_ids": {},
        "paths_map": {"answer": [["edge_1"]]},
        "stream_buffers": [],
        "stream_positions": [],
        "closed_streams": [],
    })
    event_filter = ResponseStreamFilter()
    event_filter.loads(legacy_snapshot)
    event_filter.initialize(_context(graph))
    chunk = _stream_chunk("saved", is_final=False)

    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(chunk)) == []

    # An upgraded checkpoint must remain usable after retiring its legacy readers.
    upgraded_snapshot = event_filter.dumps()
    monkeypatch.setitem(
        sys.modules, "graphon.engine.filter.builtin.response_stream.v1", None
    )
    monkeypatch.setitem(
        sys.modules, "graphon.engine.filter.builtin.response_stream.v2", None
    )
    response_stream_package = sys.modules[
        "graphon.engine.filter.builtin.response_stream"
    ]
    monkeypatch.delattr(response_stream_package, "v1", raising=False)
    monkeypatch.delattr(response_stream_package, "v2", raising=False)
    restored_filter = ResponseStreamFilter()
    restored_filter.loads(upgraded_snapshot)
    restored_filter.initialize(_context(graph))
    event_filter = restored_filter

    remaining_edge_event = _edge_taken(edge_id="edge_1")
    assert list(event_filter.on_event(remaining_edge_event)) == [chunk]
    assert list(event_filter.on_event(remaining_edge_event)) == []
    final_chunk = _stream_chunk(" end")
    assert list(event_filter.on_event(final_chunk)) == [final_chunk]


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_response_stream_filter_restores_legacy_stream_progress(version: str) -> None:
    response_graph = _variable_response_graph(
        segments=[
            TextSegment(text="prefix "),
            VariableSegment(selector=["source", "first"]),
            TextSegment(text=" / "),
            VariableSegment(selector=["source", "second"]),
            TextSegment(text=" suffix"),
        ],
        edge_id="edge_0",
    )
    waiting_answer = _TestNode(
        "waiting-answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[TextSegment(text="next")]),
    )
    later_answer = _TestNode(
        "later-answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[TextSegment(text="last")]),
    )
    graph = _TestGraph(
        nodes={
            **response_graph.nodes,
            "start": _TestNode("start"),
            "branch": _TestNode("branch", execution_type=NodeExecutionType.BRANCH),
            waiting_answer.id: waiting_answer,
            later_answer.id: later_answer,
        },
        edges={
            **response_graph.edges,
            "edge_1": _TestEdge("edge_1", "start", "source"),
            "edge_2": _TestEdge("edge_2", "start", "branch"),
            "edge_3": _TestEdge("edge_3", "branch", waiting_answer.id),
            "edge_4": _TestEdge("edge_4", "branch", later_answer.id),
        },
        root_node_id="start",
    )
    # Pin event fields to c3870df (v1) and 1821d48 (v2), before frame/retry metadata.
    chunk_fields: dict[str, Any] = {
        "id": "source-run",
        "node_id": "source",
        "node_type": "code",
        "node_version": "1",
    }
    if version == "1.0":
        chunk_fields.update({"in_iteration_id": None, "in_loop_id": None})
    else:
        chunk_fields["container_id"] = ""
    # The prefix and first chunk were emitted; the second selector is still buffered.
    legacy_snapshot = json.dumps({
        "type": "ResponseStreamFilter",
        "version": version,
        "response_nodes": ["answer", "later-answer", "waiting-answer"],
        "active_session": {"node_id": "answer", "index": 1},
        "waiting_sessions": [
            {"node_id": "waiting-answer", "index": 0},
            {"node_id": "later-answer", "index": 0},
        ],
        "pending_sessions": [],
        "node_execution_ids": {
            "answer": "answer-run",
            "source": "source-run",
            "waiting-answer": "waiting-run",
            "later-answer": "later-run",
        },
        "paths_map": {
            "answer": [[]],
            "waiting-answer": [[]],
            "later-answer": [[]],
        },
        "stream_buffers": [
            {
                "selector": ["source", "first"],
                "events": [
                    {
                        **chunk_fields,
                        "selector": ["source", "first"],
                        "chunk": "fi",
                        "is_final": False,
                    }
                ],
            },
            {
                "selector": ["source", "second"],
                "events": [
                    {
                        **chunk_fields,
                        "selector": ["source", "second"],
                        "chunk": "second",
                        "is_final": True,
                    }
                ],
            },
        ],
        "stream_positions": [{"selector": ["source", "first"], "position": 1}],
        "closed_streams": [["source", "second"]],
    })
    event_filter = ResponseStreamFilter()
    event_filter.loads(legacy_snapshot)
    event_filter.initialize(_context(graph))

    assert list(event_filter.flush()) == []
    output = list(
        event_filter.on_event(_stream_chunk("rst", selector=["source", "first"]))
    )

    chunks = [event for event in output if isinstance(event, NodeRunStreamChunkEvent)]
    assert [(event.id, event.chunk) for event in chunks] == [
        ("source-run", "rst"),
        ("answer-run", " / "),
        ("source-run", "second"),
        ("answer-run", " suffix"),
        ("waiting-run", "next"),
        ("later-run", "last"),
    ]
    assert chunks[-1].selector == ["later-answer", "answer"]
    assert chunks[-1].is_final is True
    assert list(event_filter.flush()) == []


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
        filter_engine_events(
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
        filter_engine_events(
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
