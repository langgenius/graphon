import random
from collections.abc import Sequence
from typing import Any

import pytest

from graphon.engine.filter import ResponseStreamFilter
from graphon.enums import NodeExecutionType
from graphon.nodes.base.template import Template, VariableSegment
from tests.engine.test_response_stream_filter import (
    _context,
    _TestEdge,
    _TestGraph,
    _TestNode,
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
    current_path: list[str] | None = None,
    visited: set[str] | None = None,
) -> list[list[str]]:
    """Enumerate paths with an independent DFS, without reverse pruning."""
    stack = [
        (
            current_node_id,
            [] if current_path is None else current_path.copy(),
            set() if visited is None else visited.copy(),
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


def _diamond_with_dead_branch(*, layers: int = 6) -> _CountingGraph:
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


def test_response_filter_does_not_expand_dead_branches() -> None:
    graph = _diamond_with_dead_branch()
    baseline_paths = _reference_paths(graph, "root", "answer")
    baseline_expansions = len(graph.outgoing_calls)
    graph.outgoing_calls.clear()
    event_filter = ResponseStreamFilter()

    event_filter.initialize(_context(graph))

    assert baseline_paths == [["root-live", "live-answer"]]
    assert len(graph.outgoing_calls) < baseline_expansions


def test_response_filter_classifies_each_path_edge_once_per_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nodes = {
        node_id: _TestNode(node_id) for node_id in ("root", "left", "right", "merge")
    }
    nodes["answer"] = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["root", "answer"])]),
    )
    edges = {
        edge.id: edge
        for edge in (
            _TestEdge("root-left", "root", "left"),
            _TestEdge("root-right", "root", "right"),
            _TestEdge("left-merge", "left", "merge"),
            _TestEdge("right-merge", "right", "merge"),
            _TestEdge("merge-answer", "merge", "answer"),
        )
    }
    graph = _TestGraph(
        nodes=nodes,
        edges=edges,
        root_node_id="root",
    )
    event_filter = ResponseStreamFilter()
    calls: list[str] = []
    original = event_filter._is_blocking_edge

    def classify_once(edge_id: str, selectors: set[tuple[str, ...]]) -> bool:
        calls.append(edge_id)
        return original(edge_id, selectors)

    monkeypatch.setattr(event_filter, "_is_blocking_edge", classify_once)

    event_filter.initialize(_context(graph))

    assert calls == [
        "root-left",
        "left-merge",
        "merge-answer",
        "root-right",
        "right-merge",
    ]


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
    edges = {
        f"edge-{index}": _TestEdge(f"edge-{index}", tail, head)
        for index, (tail, head) in enumerate(edge_pairs)
    }
    return _TestGraph(nodes=nodes, edges=edges, root_node_id="n0")


@pytest.mark.parametrize("seed", range(24))
def test_response_filter_paths_match_reference_dfs_for_cyclic_graphs(
    seed: int,
) -> None:
    graph = _random_cyclic_graph(seed)
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))

    actual = event_filter._find_all_paths("n0", "n5")
    expected = _reference_paths(graph, "n0", "n5")

    assert actual == expected


def test_response_filter_preserves_seed_path_and_visited_nodes() -> None:
    nodes = {node_id: _TestNode(node_id) for node_id in ("root", "cycle", "branch")}
    nodes["answer"] = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template(segments=[VariableSegment(selector=["root", "answer"])]),
    )
    edges = {
        edge.id: edge
        for edge in (
            _TestEdge("root-cycle", "root", "cycle"),
            _TestEdge("cycle-root", "cycle", "root"),
            _TestEdge("cycle-branch", "cycle", "branch"),
            _TestEdge("branch-answer", "branch", "answer"),
        )
    }
    graph = _TestGraph(nodes=nodes, edges=edges, root_node_id="root")
    event_filter = ResponseStreamFilter()
    event_filter.initialize(_context(graph))

    actual = event_filter._find_all_paths(
        "root",
        "answer",
        current_path=["seed-edge"],
        visited={"root"},
    )
    expected = _reference_paths(
        graph,
        "root",
        "answer",
        current_path=["seed-edge"],
        visited={"root"},
    )

    assert (
        actual
        == expected
        == [["seed-edge", "root-cycle", "cycle-branch", "branch-answer"]]
    )
