from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast
from unittest.mock import Mock

import pytest

from graphon.entities.graph_config import NodeConfigDict
from graphon.enums import NodeExecutionType, NodeState
from graphon.graph.graph import Graph
from graphon.graph.validation import GraphValidationError
from graphon.nodes.base.node import Node


@dataclass(slots=True)
class _RecordingNodeFactory:
    created_node_ids: list[str] = field(default_factory=list)

    def create_node(self, node_config: NodeConfigDict) -> Node:
        node_id = node_config["id"]
        self.created_node_ids.append(node_id)

        data = node_config["data"]
        node_type = data.get("type")
        node = cast(Any, Mock(spec=Node))
        node.id = node_id
        node.node_type = node_type
        node.execution_type = (
            NodeExecutionType.ROOT
            if node_type in {"start", "iteration-start", "loop-start"}
            else NodeExecutionType.EXECUTABLE
        )
        node.error_strategy = None
        node.state = NodeState.UNKNOWN
        return node


def _node(
    node_id: str,
    *,
    node_type: str = "answer",
    container_id: str = "",
) -> dict[str, Any]:
    return {
        "id": node_id,
        "data": {"type": node_type, "container_id": container_id},
    }


def _edge(source: str, target: str) -> dict[str, str]:
    return {"source": source, "target": target}


def _scoped_graph_config() -> dict[str, Any]:
    return {
        "nodes": [
            _node("start", node_type="start"),
            _node("container-a", node_type="loop"),
            _node("container-b", node_type="iteration"),
            _node("end"),
            _node("a-start", node_type="start", container_id="container-a"),
            _node(
                "nested-container",
                node_type="iteration",
                container_id="container-a",
            ),
            _node("a-end", container_id="container-a"),
            _node(
                "nested-start",
                node_type="start",
                container_id="nested-container",
            ),
            _node("nested-end", container_id="nested-container"),
            _node("b-start", node_type="start", container_id="container-b"),
            _node("b-end", container_id="container-b"),
        ],
        "edges": [
            _edge("start", "container-a"),
            _edge("container-a", "container-b"),
            _edge("container-b", "end"),
            _edge("a-start", "nested-container"),
            _edge("nested-container", "a-end"),
            _edge("nested-start", "nested-end"),
            _edge("b-start", "b-end"),
        ],
        "viewport": {"x": 10, "y": 20},
    }


def _config_node_ids(graph_config: Mapping[str, Any]) -> set[str]:
    return {node["id"] for node in graph_config["nodes"]}


def _config_edges(graph_config: Mapping[str, Any]) -> set[tuple[str, str]]:
    return {(edge["source"], edge["target"]) for edge in graph_config["edges"]}


def _graph_edges(graph: Graph) -> set[tuple[str, str]]:
    return {(edge.tail, edge.head) for edge in graph.edges.values()}


def test_graph_init_materializes_only_root_scope_by_default() -> None:
    graph_config = _scoped_graph_config()
    node_factory = _RecordingNodeFactory()

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="start",
    )

    assert set(graph.nodes) == {"start", "container-a", "container-b", "end"}
    assert set(node_factory.created_node_ids) == set(graph.nodes)
    assert _graph_edges(graph) == {
        ("start", "container-a"),
        ("container-a", "container-b"),
        ("container-b", "end"),
    }
    assert graph.graph_config is not None
    assert all(node.graph_config is graph.graph_config for node in graph.nodes.values())
    assert _config_node_ids(graph.graph_config) == {
        node["id"] for node in graph_config["nodes"]
    }
    assert graph.graph_config["viewport"] == graph_config["viewport"]


def test_graph_init_scopes_execution_and_retains_only_its_subtree_config() -> None:
    graph_config = _scoped_graph_config()
    node_factory = _RecordingNodeFactory()

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="a-start",
        container_id="container-a",
    )

    assert set(graph.nodes) == {"a-start", "nested-container", "a-end"}
    assert _graph_edges(graph) == {
        ("a-start", "nested-container"),
        ("nested-container", "a-end"),
    }
    assert graph.graph_config is not None
    assert all(node.graph_config is graph.graph_config for node in graph.nodes.values())
    assert _config_node_ids(graph.graph_config) == {
        "a-start",
        "nested-container",
        "a-end",
        "nested-start",
        "nested-end",
    }
    assert _config_edges(graph.graph_config) == {
        ("a-start", "nested-container"),
        ("nested-container", "a-end"),
        ("nested-start", "nested-end"),
    }


def test_graph_init_rejects_edges_crossing_container_scopes() -> None:
    graph_config = _scoped_graph_config()
    graph_config["edges"].append(_edge("container-a", "a-start"))

    with pytest.raises(
        ValueError,
        match=(
            r"Edge 'container-a->a-start' crosses container scopes "
            r"'' and 'container-a'"
        ),
    ):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="start",
        )


def test_graph_init_rejects_orphan_scopes_and_unknown_edges() -> None:
    graph_config = _scoped_graph_config()
    graph_config["nodes"].append(_node("orphan", container_id="missing"))
    with pytest.raises(ValueError, match="orphan"):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="start",
        )

    graph_config = _scoped_graph_config()
    graph_config["edges"].append(_edge("ghost-a", "ghost-b"))
    with pytest.raises(GraphValidationError):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="start",
        )


def test_graph_init_requires_container_id_for_nested_legacy_fields() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": "start"}},
            {"id": "loop", "data": {"type": "loop"}},
            {
                "id": "loop-start",
                "data": {"type": "loop-start", "loop_id": "loop"},
            },
            {
                "id": "iteration",
                "data": {"type": "iteration", "loop_id": "loop"},
            },
            {
                "id": "iteration-start",
                "data": {
                    "type": "iteration-start",
                    "loop_id": "loop",
                    "iteration_id": "iteration",
                },
            },
            {
                "id": "stop",
                "data": {
                    "type": "loop-end",
                    "loop_id": "loop",
                    "iteration_id": "iteration",
                },
            },
        ],
        "edges": [
            _edge("start", "loop"),
            _edge("loop-start", "iteration"),
            _edge("iteration-start", "stop"),
        ],
    }

    with pytest.raises(
        ValueError,
        match=r"Nested container nodes must set data\.container_id",
    ):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="iteration-start",
            container_id="iteration",
        )
