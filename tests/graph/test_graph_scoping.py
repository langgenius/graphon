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
        if node_type in {"loop", "iteration", "custom-container"}:
            node.execution_type = NodeExecutionType.CONTAINER
        elif node_type in {"start", "iteration-start", "loop-start"}:
            node.execution_type = NodeExecutionType.ROOT
        else:
            node.execution_type = NodeExecutionType.EXECUTABLE
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


def _edge(source: str, target: str) -> dict[str, Any]:
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


def test_scoped_graphs_preserve_full_config_edge_ids() -> None:
    """Expose legacy fallback IDs and retain them in every scoped config.

    Missing DSL IDs use their original full-config ordinal. Graph initialization
    writes those IDs only to copied configs so callers are not mutated, while
    later child and nested materializations reuse the public ``id`` field.
    """
    graph_config = _scoped_graph_config()
    node_factory = _RecordingNodeFactory()

    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=node_factory,
        root_node_id="start",
    )
    assert {
        edge_id: (edge.tail, edge.head) for edge_id, edge in root_graph.edges.items()
    } == {
        "edge_0": ("start", "container-a"),
        "edge_1": ("container-a", "container-b"),
        "edge_2": ("container-b", "end"),
    }
    assert all("id" not in edge for edge in graph_config["edges"])
    assert root_graph.graph_config is not None
    assert [edge["id"] for edge in root_graph.graph_config["edges"]] == [
        "edge_0",
        "edge_1",
        "edge_2",
        "edge_3",
        "edge_4",
        "edge_5",
        "edge_6",
    ]
    assert all(
        "__graphon_edge_id" not in edge for edge in root_graph.graph_config["edges"]
    )

    child_graph = Graph.init(
        graph_config=root_graph.graph_config,
        node_factory=node_factory,
        root_node_id="a-start",
        container_id="container-a",
    )
    assert {
        edge_id: (edge.tail, edge.head) for edge_id, edge in child_graph.edges.items()
    } == {
        "edge_3": ("a-start", "nested-container"),
        "edge_4": ("nested-container", "a-end"),
    }
    assert child_graph.graph_config is not None
    assert _config_edges(child_graph.graph_config) == {
        ("a-start", "nested-container"),
        ("nested-container", "a-end"),
        ("nested-start", "nested-end"),
    }

    nested_graph = Graph.init(
        graph_config=child_graph.graph_config,
        node_factory=node_factory,
        root_node_id="nested-start",
        container_id="nested-container",
    )
    assert {
        edge_id: (edge.tail, edge.head) for edge_id, edge in nested_graph.edges.items()
    } == {"edge_5": ("nested-start", "nested-end")}
    assert nested_graph.graph_config is not None
    assert _config_edges(nested_graph.graph_config) == {
        ("nested-start", "nested-end"),
    }


def test_graph_init_preserves_public_edge_ids() -> None:
    graph_config = _scoped_graph_config()
    graph_config["edges"][0]["id"] = "start-to-container"
    graph_config["edges"][3]["id"] = "child-edge"

    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="start",
    )
    assert "start-to-container" in root_graph.edges
    assert root_graph.graph_config is not None

    child_graph = Graph.init(
        graph_config=root_graph.graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="a-start",
        container_id="container-a",
    )
    assert "child-edge" in child_graph.edges


@pytest.mark.parametrize(
    ("explicit_index", "explicit_id", "expected_edge_ids"),
    [
        (0, "edge_1", {"edge_1", "edge_2", "edge_3"}),
        (1, "edge_0", {"edge_0", "edge_1", "edge_2"}),
    ],
)
def test_generated_edge_ids_reserve_explicit_ids(
    explicit_index: int,
    explicit_id: str,
    expected_edge_ids: set[str],
) -> None:
    """Keep fallback IDs distinct from every supplied public edge ID.

    Mixed DSLs may give stable IDs to only some edges. Graph initialization
    must reserve all supplied IDs before filling omissions so both an explicit
    ID before an unnamed edge and one later in the config remain loadable.
    """
    graph_config = _scoped_graph_config()
    graph_config["edges"][explicit_index]["id"] = explicit_id

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="start",
    )

    assert set(graph.edges) == expected_edge_ids


def test_generated_edge_ids_reserve_explicit_ids_only_in_their_graph() -> None:
    """Allow a child graph's explicit ID to match a root fallback ID.

    Edge IDs are local to a graph and become globally addressable only when
    paired with a frame ID. Reserving a child's public ID must therefore not
    renumber an otherwise stable root fallback ID.
    """
    graph_config = _scoped_graph_config()
    graph_config["edges"][3]["id"] = "edge_0"

    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="start",
    )
    assert root_graph.edges["edge_0"].tail == "start"
    assert root_graph.graph_config is not None

    child_graph = Graph.init(
        graph_config=root_graph.graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="a-start",
        container_id="container-a",
    )

    assert child_graph.edges["edge_0"].tail == "a-start"


def test_graph_init_rejects_duplicate_edge_ids_in_one_scope() -> None:
    graph_config = _scoped_graph_config()
    graph_config["edges"][0]["id"] = "edge-duplicate"
    graph_config["edges"][1]["id"] = "edge-duplicate"

    with pytest.raises(ValueError, match="Duplicate graph edge ID: edge-duplicate"):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="start",
        )


def test_child_graphs_may_reuse_edge_ids() -> None:
    graph_config = _scoped_graph_config()
    graph_config["edges"][3]["id"] = "local-edge"
    graph_config["edges"][6]["id"] = "local-edge"

    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="start",
    )
    assert root_graph.graph_config is not None

    child_a = Graph.init(
        graph_config=root_graph.graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="a-start",
        container_id="container-a",
    )
    child_b = Graph.init(
        graph_config=root_graph.graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="b-start",
        container_id="container-b",
    )

    assert "local-edge" in child_a.edges
    assert "local-edge" in child_b.edges


@pytest.mark.parametrize("edge_id", ["", None, 1])
def test_graph_init_rejects_invalid_public_edge_ids(edge_id: object) -> None:
    graph_config = _scoped_graph_config()
    graph_config["edges"][0]["id"] = edge_id

    with pytest.raises(ValueError, match="must be a non-empty string"):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="start",
        )


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


@pytest.mark.parametrize(
    ("owner_type", "ownership_field"),
    [
        ("custom-container", "container_id"),
        ("loop", "loop_id"),
    ],
)
def test_graph_init_accepts_container_scope_owners(
    owner_type: str,
    ownership_field: str,
) -> None:
    """Accept canonical and legacy child ownership for any container node.

    Container semantics come from the node factory's ``execution_type`` rather
    than a hard-coded node type, so downstream container implementations remain
    valid while legacy Loop ownership continues to load during migration.
    """
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": "start"}},
            {"id": "owner", "data": {"type": owner_type}},
            {
                "id": "child",
                "data": {"type": "answer", ownership_field: "owner"},
            },
        ],
        "edges": [_edge("start", "owner")],
    }

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id="start",
    )

    assert set(graph.nodes) == {"start", "owner"}


def test_graph_init_rejects_non_container_scope_owner() -> None:
    """Reject a child scope whose direct owner cannot create a child frame.

    Scoping removes child nodes from the parent's executable graph. Allowing an
    ordinary node to own that scope would therefore make execution succeed while
    silently skipping the child, so Graph initialization must fail first.
    """
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": "start"}},
            {"id": "ordinary", "data": {"type": "answer"}},
            {
                "id": "child",
                "data": {"type": "answer", "container_id": "ordinary"},
            },
        ],
        "edges": [_edge("start", "ordinary")],
    }

    with pytest.raises(
        ValueError,
        match="Node 'ordinary' owns child nodes but is not a container",
    ):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="start",
        )


@pytest.mark.parametrize(
    ("outer_id", "outer_type", "inner_id", "inner_type", "inner_owner_field"),
    [
        ("loop", "loop", "iteration", "iteration", "loop_id"),
        ("iteration", "iteration", "loop", "loop", "iteration_id"),
    ],
)
def test_graph_init_accepts_nested_legacy_fields_without_container_id(
    outer_id: str,
    outer_type: str,
    inner_id: str,
    inner_type: str,
    inner_owner_field: str,
) -> None:
    inner_start_id = f"{inner_id}-start"
    graph_config = {
        "nodes": [
            {"id": outer_id, "data": {"type": outer_type}},
            {
                "id": inner_id,
                "data": {"type": inner_type, inner_owner_field: outer_id},
            },
            {
                "id": inner_start_id,
                "data": {
                    "type": f"{inner_type}-start",
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
            _edge(inner_start_id, "stop"),
        ],
    }

    graph = Graph.init(
        graph_config=graph_config,
        node_factory=_RecordingNodeFactory(),
        root_node_id=inner_start_id,
        container_id=inner_id,
    )

    assert set(graph.nodes) == {inner_start_id, "stop"}
    assert _graph_edges(graph) == {(inner_start_id, "stop")}


def test_graph_init_rejects_unrelated_legacy_container_owners() -> None:
    """Reject two legacy owners when neither container encloses the other."""
    graph_config = {
        "nodes": [
            {"id": "loop", "data": {"type": "loop"}},
            {"id": "iteration", "data": {"type": "iteration"}},
            {
                "id": "nested",
                "data": {
                    "type": "start",
                    "loop_id": "loop",
                    "iteration_id": "iteration",
                },
            },
        ],
        "edges": [],
    }

    with pytest.raises(ValueError, match="ambiguous legacy container owners"):
        Graph.init(
            graph_config=graph_config,
            node_factory=_RecordingNodeFactory(),
            root_node_id="nested",
            container_id="iteration",
        )
