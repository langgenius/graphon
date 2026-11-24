"""Behavioral equivalence to the old path gate; no performance assertions."""

from dataclasses import dataclass
from itertools import pairwise

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from graphon.engine.filter import ResponseStreamFilter
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.engine_events.traversal import GraphEdgeSkippedEvent
from graphon.enums import NodeExecutionType
from graphon.nodes.base.template import Template, TextSegment
from tests.engine.test_response_stream_filter import (
    _context,
    _edge_taken,
    _TestEdge,
    _TestGraph,
    _TestNode,
)


@dataclass(frozen=True)
class _DagCase:
    node_count: int
    edges: tuple[tuple[int, int], ...]
    blocking_nodes: frozenset[int]
    edge_decisions: tuple[tuple[int, bool], ...]


@st.composite
def _generate_dag_cases(
    draw: st.DrawFn, graph_shape: str, blocking_pattern: str
) -> _DagCase:
    # All live nodes lie on root -> (arm1 OR arm2) -> merge -> suffix -> reply.
    # Side sinks cannot reach reply; detached predecessors cannot be reached
    # from root. The three shapes exercise both sides of reachability pruning.
    extra_node_count = {"all_reachable": 0, "dead_branches": 1, "mixed": 2}[graph_shape]
    live_node_count = draw(st.integers(min_value=6, max_value=10 - extra_node_count))
    response_node_index = live_node_count + extra_node_count - 1
    suffix_nodes = [*range(3, live_node_count - 1), response_node_index]
    edge_endpoints = [(0, 1), (0, 2), (1, 3), (2, 3), *pairwise(suffix_nodes)]
    # Forward-only arcs keep the domain acyclic and preserve the AND gates at
    # nodes 3/4. Parallel handles add alternative paths without new vertices.
    shortcut_edges = [(1, 2), *zip(suffix_nodes[1:], suffix_nodes[3:], strict=False)]
    edge_endpoints.extend(draw(st.sets(st.sampled_from(shortcut_edges))))
    if draw(st.booleans()):
        edge_endpoints.append((1, 3))
    if extra_node_count:
        sink_node_index = live_node_count - 1
        edge_endpoints.append((
            draw(st.integers(min_value=0, max_value=3)),
            sink_node_index,
        ))
    if extra_node_count == 2:
        detached_node_index = live_node_count
        edge_endpoints.append((detached_node_index, response_node_index))
    blocking_nodes = {
        "and": frozenset({3, 4}),
        "or": frozenset({1, 2}),
        "and_or": frozenset({1, 2, 3}),
    }[blocking_pattern]
    # Each edge is considered once in a random order, followed by repeated and
    # opposite decisions. This includes skip-before-taken and taken-before-skip.
    edge_order = draw(st.permutations(tuple(range(len(edge_endpoints)))))
    taken_by_edge = draw(
        st.lists(
            st.booleans(), min_size=len(edge_endpoints), max_size=len(edge_endpoints)
        )
    )
    edge_decisions = [(index, taken_by_edge[index]) for index in edge_order]
    repeated_edge_indices = draw(
        st.lists(
            st.integers(min_value=0, max_value=len(edge_endpoints) - 1), max_size=6
        )
    )
    edge_decisions.extend(
        (index, taken_by_edge[index]) for index in repeated_edge_indices
    )
    edge_decisions.extend(
        (index, not taken_by_edge[index]) for index in reversed(edge_order)
    )
    return _DagCase(
        response_node_index + 1,
        tuple(edge_endpoints),
        blocking_nodes,
        tuple(edge_decisions),
    )


def _enumerate_old_blocking_paths(case: _DagCase) -> list[list[int]]:
    """Return old DFS paths projected onto blocking edges, as in 069cd554."""

    def enumerate_paths(
        node_index: int, path: list[int], visited_nodes: set[int]
    ) -> list[list[int]]:
        if node_index == case.node_count - 1:
            return [path.copy()]
        next_visited_nodes = {node_index, *visited_nodes}
        return [
            complete_path
            for index, (tail, head) in enumerate(case.edges)
            if tail == node_index and head not in next_visited_nodes
            for complete_path in enumerate_paths(
                head, [*path, index], next_visited_nodes
            )
        ]

    return [
        [index for index in path if case.edges[index][0] in case.blocking_nodes]
        for path in enumerate_paths(0, [], set())
    ]


def _build_graph_for_case(case: _DagCase) -> _TestGraph:
    nodes = {
        str(index): _TestNode(
            str(index),
            execution_type=NodeExecutionType.BRANCH
            if index in case.blocking_nodes
            else NodeExecutionType.EXECUTABLE,
        )
        for index in range(case.node_count)
    }
    response_node_id = str(case.node_count - 1)
    nodes[response_node_id] = _TestNode(
        response_node_id,
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[TextSegment(text="ready")]),
    )
    edges = {
        str(index): _TestEdge(str(index), str(tail), str(head))
        for index, (tail, head) in enumerate(case.edges)
    }
    return _TestGraph(nodes=nodes, edges=edges, root_node_id="0")


@pytest.mark.parametrize("graph_shape", ["all_reachable", "dead_branches", "mixed"])
@pytest.mark.parametrize("blocking_pattern", ["and", "or", "and_or"])
@settings(max_examples=100, derandomize=True, deadline=None)
@given(data=st.data())
def test_gating_matches_old_paths_at_every_event_prefix(
    graph_shape: str, blocking_pattern: str, data: st.DataObject
) -> None:
    case = data.draw(_generate_dag_cases(graph_shape, blocking_pattern))
    graph = _build_graph_for_case(case)
    blocking_paths = _enumerate_old_blocking_paths(case)
    assert blocking_paths
    assert all(blocking_paths)
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    assert list(event_filter.on_event(GraphRunStartedEvent())) == [
        GraphRunStartedEvent()
    ]
    emitted = False
    for index, was_taken in case.edge_decisions:
        edge = graph.edges[str(index)]
        if was_taken:
            for path in blocking_paths:
                if index in path:
                    path.remove(index)
            event = _edge_taken(
                edge_id=edge.id, source_id=edge.tail, answer_id=edge.head
            )
        else:
            blocking_paths = [path for path in blocking_paths if index not in path]
            event = GraphEdgeSkippedEvent(
                frame_id="root",
                edge_id=edge.id,
                source_node_id=edge.tail,
                target_node_id=edge.head,
            )
        ready = any(not path for path in blocking_paths)
        chunks = [
            item.chunk
            for item in event_filter.on_event(event)
            if isinstance(item, NodeRunStreamChunkEvent)
        ]
        assert chunks == (["ready"] if ready and not emitted else [])
        emitted = emitted or ready
