import random
from collections.abc import Sequence
from typing import Any

import pytest

from graphon.engine.filter import ResponseStreamFilter
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.node import NodeRunStreamChunkEvent
from graphon.engine_events.traversal import GraphEdgeTakenEvent
from graphon.enums import NodeExecutionType, NodeState
from graphon.nodes.base.template import Template, VariableSegment
from tests.engine.test_response_stream_filter import (
    _context,
    _edge_taken,
    _stream_chunk,
    _TestEdge,
    _TestGraph,
    _TestNode,
    _variable_response_graph,
)


class _CountingGraph(_TestGraph):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.outgoing_calls: list[str] = []

    def get_outgoing_edges(self, node_id: str) -> Sequence[_TestEdge]:
        self.outgoing_calls.append(node_id)
        return super().get_outgoing_edges(node_id)


def _reference_paths(
    graph: _TestGraph,
    current_node_id: str,
    target_node_id: str,
) -> list[list[str]]:
    """Enumerate paths with an independent DFS, without reverse pruning."""
    stack = [
        (
            current_node_id,
            [],
            set(),
        )
    ]
    paths: list[list[str]] = []
    while stack:
        node_id, path, seen = stack.pop()
        if node_id == target_node_id:
            paths.append(path)
            continue

        next_seen = {node_id, *seen}
        outgoing = list(graph.get_outgoing_edges(node_id))
        for edge in reversed(outgoing):
            if edge.head in next_seen:
                continue
            stack.append((edge.head, [*path, edge.id], next_seen.copy()))
    return paths


def _diamond_with_dead_branch(*, layers: int = 8) -> _CountingGraph:
    nodes = {node_id: _TestNode(node_id) for node_id in ("root", "live", "dead")}
    nodes["answer"] = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["root", "answer"])]),
    )
    edges = {
        edge.id: edge
        for edge in (
            _TestEdge("root-live", "root", "live"),
            _TestEdge("root-dead", "root", "dead"),
            _TestEdge("live-answer", "live", "answer"),
        )
    }
    current_node_id = "dead"
    for layer in range(layers):
        left_node_id = f"dead-{layer}-left"
        right_node_id = f"dead-{layer}-right"
        merge_node_id = f"dead-{layer}-merge"
        nodes.update({
            left_node_id: _TestNode(left_node_id),
            right_node_id: _TestNode(right_node_id),
            merge_node_id: _TestNode(merge_node_id),
        })
        for edge in (
            _TestEdge(
                f"{current_node_id}-left",
                current_node_id,
                left_node_id,
            ),
            _TestEdge(
                f"{current_node_id}-right",
                current_node_id,
                right_node_id,
            ),
            _TestEdge(
                f"{left_node_id}-merge",
                left_node_id,
                merge_node_id,
            ),
            _TestEdge(
                f"{right_node_id}-merge",
                right_node_id,
                merge_node_id,
            ),
        ):
            edges[edge.id] = edge
        current_node_id = merge_node_id

    return _CountingGraph(
        nodes=nodes,
        edges=edges,
        root_node_id="root",
    )


def _reference_blocking_paths(
    graph: _TestGraph, selector: Sequence[str]
) -> list[set[str]]:
    response = next(
        node
        for node in graph.nodes.values()
        if node.execution_type == NodeExecutionType.RESPONSE
    )
    reference_graph = _TestGraph(
        nodes=graph.nodes, edges=graph.edges, root_node_id=graph.root_node.id
    )
    selectors = {tuple(selector[:2])}
    blocking_types = {
        NodeExecutionType.BRANCH,
        NodeExecutionType.CONTAINER,
        NodeExecutionType.RESPONSE,
    }
    blocking_edge_ids = {
        edge.id
        for edge in graph.edges.values()
        if graph.nodes[edge.tail].execution_type in blocking_types
        or graph.nodes[edge.tail].blocks_variable_output(selectors)
    }
    return [
        set(path) & blocking_edge_ids
        for path in _reference_paths(reference_graph, graph.root_node.id, response.id)
    ]


def _assert_activation_matches_paths(graph: _TestGraph, *, seed: int = 0) -> None:
    response = next(
        node
        for node in graph.nodes.values()
        if node.execution_type == NodeExecutionType.RESPONSE
    )
    segment = response.get_streaming_template().segments[0]
    assert isinstance(segment, VariableSegment)
    selector = segment.selector
    blocking_paths = _reference_blocking_paths(graph, selector)
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    chunk = _stream_chunk("value", source_id=selector[0], selector=selector)
    assert list(event_filter.on_event(chunk)) == []
    edge_ids = list(graph.edges)
    random.Random(seed).shuffle(edge_ids)  # ruff: ignore[suspicious-non-cryptographic-random-usage] - deterministic traversal order
    events: list[EngineEvent] = [GraphRunStartedEvent()]
    for edge_id in [*edge_ids, *edge_ids[:2]]:
        edge = graph.edges[edge_id]
        events.append(
            _edge_taken(edge_id=edge.id, source_id=edge.tail, answer_id=edge.head)
        )
    taken_edge_ids: set[str] = set()
    emitted = False
    for event in events:
        if isinstance(event, GraphEdgeTakenEvent):
            taken_edge_ids.add(event.edge_id)
        ready = any(path <= taken_edge_ids for path in blocking_paths)
        output = [
            value.chunk
            for value in event_filter.on_event(event)
            if isinstance(value, NodeRunStreamChunkEvent)
        ]
        assert output == (["value"] if ready and not emitted else [])
        emitted = emitted or ready


@pytest.mark.parametrize("skipped_in_live_graph", [False, True])
@pytest.mark.parametrize("selected_edge_id", ["edge-1", "failure"])
def test_response_filter_keeps_parallel_handles_and_ignores_live_edge_state(
    skipped_in_live_graph: bool,
    selected_edge_id: str,
) -> None:
    graph = _variable_response_graph()
    graph.nodes["source"].execution_type = NodeExecutionType.BRANCH
    success_edge = graph.edges["edge-1"]
    success_edge.source_handle = "success"
    failure_edge = _TestEdge("failure", "source", "answer")
    failure_edge.source_handle = "failure"
    graph.edges[failure_edge.id] = failure_edge
    if skipped_in_live_graph:
        success_edge.state = NodeState.SKIPPED
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))
    list(event_filter.on_event(GraphRunStartedEvent()))
    chunk = _stream_chunk("value")
    assert list(event_filter.on_event(chunk)) == []
    selected_edge = graph.edges[selected_edge_id]
    taken_event = _edge_taken(edge_id=selected_edge.id)
    taken_event.source_handle = selected_edge.source_handle
    assert list(event_filter.on_event(taken_event)) == [chunk]


def test_response_filter_reinitialization_recomputes_selector_dependent_blocking() -> (
    None
):
    event_filter = ResponseStreamFilter()
    first_graph = _variable_response_graph(selector=["source", "answer"])
    event_filter.initialize(_context(first_graph))
    list(event_filter.on_event(GraphRunStartedEvent()))
    assert list(event_filter.on_event(_stream_chunk("blocked"))) == []
    second_graph = _variable_response_graph(selector=["source", "text"])
    event_filter.initialize(_context(second_graph))
    list(event_filter.on_event(GraphRunStartedEvent()))
    chunk = _stream_chunk("streaming", selector=["source", "text"])
    assert list(event_filter.on_event(chunk)) == [chunk]


def test_response_filter_activates_root_response_with_backedge() -> None:
    root = _TestNode(
        "root",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["root", "answer"])]),
    )
    graph = _TestGraph(
        nodes={"root": root, "branch": _TestNode("branch")},
        edges={
            "out": _TestEdge("out", "root", "branch"),
            "back": _TestEdge("back", "branch", "root"),
        },
        root_node_id="root",
    )
    _assert_activation_matches_paths(graph)


def test_response_filter_bounds_work_with_a_dead_diamond_branch() -> None:
    graph = _diamond_with_dead_branch()
    _assert_activation_matches_paths(graph)
    assert len(graph.outgoing_calls) <= 8 * (len(graph.nodes) + len(graph.edges))


def _random_cyclic_graph(seed: int) -> _TestGraph:
    rng = random.Random(seed)  # ruff: ignore[suspicious-non-cryptographic-random-usage] - deterministic test graph generation
    node_ids = [f"n{index}" for index in range(6)]
    edge_pairs = [
        ("n0", "n1"),
        ("n1", "n2"),
        ("n2", "n1"),
        ("n2", "n5"),
    ]
    used_pairs = set(edge_pairs)
    for tail in node_ids:
        for head in node_ids:
            if (tail, head) not in used_pairs and rng.random() < 0.22:
                edge_pairs.append((tail, head))
                used_pairs.add((tail, head))

    nodes = {node_id: _TestNode(node_id) for node_id in node_ids}
    nodes["n5"] = _TestNode(
        "n5",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["n0", "answer"])]),
    )
    for node_id in node_ids[1:-1]:
        if rng.random() < 0.5:
            nodes[node_id].execution_type = NodeExecutionType.BRANCH
    edges = {
        f"edge-{index}": _TestEdge(f"edge-{index}", tail, head)
        for index, (tail, head) in enumerate(edge_pairs)
    }
    return _TestGraph(nodes=nodes, edges=edges, root_node_id="n0")


@pytest.mark.parametrize("seed", range(24))
def test_response_filter_activation_matches_reference_paths(seed: int) -> None:
    _assert_activation_matches_paths(_random_cyclic_graph(seed), seed=seed)
