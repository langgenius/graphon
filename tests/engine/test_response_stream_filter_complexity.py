"""Resource bounds and ordered output for the ENG-1501 diamond chain."""

import sys
from collections import deque
from collections.abc import Mapping, Sequence
from types import FunctionType, MethodType, ModuleType

import pytest

from graphon.engine.filter import ResponseStreamFilter
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import FrameStartedEvent
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.nodes.base.template import Template, TextSegment
from tests.engine.test_response_stream_filter import (
    _context,
    _edge_taken,
    _TestEdge,
    _TestGraph,
    _TestNode,
)

_STAGE_COUNTS = (6, 12, 24)
_BYTES_PER_GRAPH_ITEM = 1024


class _AdjacencyCallLimitGraph(_TestGraph):
    """Abort initialization before an exponential walk can hang the test worker."""

    def __init__(self, graph: _TestGraph) -> None:
        super().__init__(
            nodes=graph.nodes, edges=graph.edges, root_node_id=graph.root_node.id
        )
        self.adjacency_calls = 0
        # See docs/development.md#response-gating-regressions for the
        # conservative O(V*E) primitive-call budget and size-ratio derivation.
        self.adjacency_call_limit = 4 * len(self.nodes) * len(self.edges)

    def _count_adjacency_call(self) -> None:
        self.adjacency_calls += 1
        assert self.adjacency_calls <= self.adjacency_call_limit, (
            f"Initialization traversal budget exceeded: {self.adjacency_calls} "
            f"> {self.adjacency_call_limit} (V={len(self.nodes)}, E={len(self.edges)})"
        )

    def get_outgoing_edges(self, node_id: str) -> Sequence[_TestEdge]:
        self._count_adjacency_call()
        return super().get_outgoing_edges(node_id)

    def get_incoming_edges(self, node_id: str) -> Sequence[_TestEdge]:
        self._count_adjacency_call()
        return super().get_incoming_edges(node_id)


def _build_diamond_chain(stage_count: int) -> _TestGraph:
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
    # Parallel handles merge at every stage: V=n+2, E=2n+1, paths=2**n.
    assert len(nodes) == stage_count + 2
    assert len(edges) == 2 * stage_count + 1
    return _TestGraph(nodes=nodes, edges=edges, root_node_id="start")


def _collect_referenced_objects(root: object) -> dict[int, object]:
    """Visit owned Python containers/instances without following code or classes."""
    objects_by_id: dict[int, object] = {}
    pending_objects = [root]
    while pending_objects:
        value = pending_objects.pop()
        if id(value) in objects_by_id or isinstance(
            value, (type, FunctionType, MethodType, ModuleType)
        ):
            continue
        objects_by_id[id(value)] = value
        if isinstance(value, Mapping):
            pending_objects.extend(value.keys())
            pending_objects.extend(value.values())
        elif isinstance(value, (list, tuple, set, frozenset, deque)):
            pending_objects.extend(value)
        elif hasattr(value, "__dict__"):
            pending_objects.append(vars(value))
    return objects_by_id


def _measure_retained_bytes(
    event_filter: ResponseStreamFilter, borrowed_object_ids: set[int]
) -> int:
    # Including rendering/session overhead with no buffered source payload makes
    # this stricter than counting gating containers alone. Shared inputs count
    # only through the owned containers' reference slots, not their referents.
    return sum(
        sys.getsizeof(value)
        for object_id, value in _collect_referenced_objects(event_filter).items()
        if object_id not in borrowed_object_ids
    )


@pytest.mark.parametrize("stage_count", _STAGE_COUNTS)
def test_diamond_initialization_and_retained_state_are_bounded(
    stage_count: int,
) -> None:
    graph = _AdjacencyCallLimitGraph(_build_diamond_chain(stage_count))
    context = _context(graph)
    borrowed_object_ids = set(_collect_referenced_objects(graph)) | set(
        _collect_referenced_objects(context.runtime_state.variable_pool)
    )
    event_filter = ResponseStreamFilter()
    event_filter.initialize(context)
    assert 0 < graph.adjacency_calls <= graph.adjacency_call_limit
    state_size_limit = _BYTES_PER_GRAPH_ITEM * (len(graph.nodes) + len(graph.edges))

    def assert_state_size_limits() -> None:
        assert (
            _measure_retained_bytes(event_filter, borrowed_object_ids)
            <= state_size_limit
        )
        assert len(event_filter.dumps().encode()) <= state_size_limit

    assert_state_size_limits()
    list(event_filter.on_event(GraphRunStartedEvent()))
    for index in range(stage_count):
        edge = graph.edges[f"gate-{index}-true"]
        list(
            event_filter.on_event(
                _edge_taken(edge_id=edge.id, source_id=edge.tail, answer_id=edge.head)
            )
        )
        assert_state_size_limits()


def test_diamond_initialization_growth_ratios_do_not_increase() -> None:
    adjacency_call_counts = []
    for stage_count in _STAGE_COUNTS:
        graph = _AdjacencyCallLimitGraph(_build_diamond_chain(stage_count))
        ResponseStreamFilter().initialize(_context(graph))
        adjacency_call_counts.append(graph.adjacency_calls)
    small_calls, medium_calls, large_calls = adjacency_call_counts
    assert small_calls > 0
    # Doubling stages has asymptotic factor four for the O(V*E) allowance.
    assert medium_calls < 4 * small_calls
    assert large_calls < 4 * medium_calls
    assert large_calls * small_calls <= medium_calls * medium_calls


@pytest.mark.parametrize("stage_count", _STAGE_COUNTS)
def test_diamond_emits_complete_sequence_before_all_blocking_edges_are_taken(
    stage_count: int,
) -> None:
    graph = _AdjacencyCallLimitGraph(_build_diamond_chain(stage_count))
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    started = GraphRunStartedEvent()
    output_events: list[EngineEvent] = list(event_filter.on_event(started))
    assert output_events == [started]
    assert (
        list(
            event_filter.on_event(
                FrameStartedEvent(
                    frame_id="root", node_execution_ids={"answer": "answer-run"}
                )
            )
        )
        == []
    )
    for index in range(stage_count):
        edge = graph.edges[f"gate-{index}-true"]
        emitted_events = list(
            event_filter.on_event(
                _edge_taken(edge_id=edge.id, source_id=edge.tail, answer_id=edge.head)
            )
        )
        expected_events = (
            [
                NodeRunStreamChunkEvent(
                    id="answer-run",
                    node_id="answer",
                    node_type=BuiltinNodeTypes.END,
                    selector=["answer", "answer"],
                    chunk="done",
                    is_final=True,
                )
            ]
            if index == stage_count - 1
            else []
        )
        assert emitted_events == expected_events
        output_events.extend(emitted_events)
    assert len(output_events) == 2
    assert isinstance(output_events[1], NodeRunStreamChunkEvent)
    # Every false handle is still unsatisfied when the first response chunk
    # arrives. Requiring every incoming edge at a merge would delay that chunk.
    for index in range(stage_count):
        edge = graph.edges[f"gate-{index}-false"]
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
    assert list(event_filter.flush()) == []
