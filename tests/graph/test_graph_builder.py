from typing import cast

import pytest

from graphon import graph
from graphon.graph import EdgeDefinition, Graph, NodeDefinition
from graphon.graph.validation import GraphValidationError


def test_build_a_branch_and_join_from_node_definitions() -> None:
    start = NodeDefinition("start", "start")
    left = NodeDefinition("left", "answer", config={"answer": "left"})
    right = NodeDefinition("right", "answer", config={"answer": "right"})
    end = NodeDefinition("end", "end", config={"outputs": []})
    builder = Graph.new().add_root(start)
    start_only = builder.build()
    definition = (
        builder
        .add_node(left, source_handle="yes")
        .add_node(right, from_node_id="start", source_handle="no")
        .add_node(end)
        .connect(tail="left", head="end")
        .build()
    )

    assert isinstance(builder, graph.GraphBuilder)
    assert start_only == Graph(nodes=[start])
    assert definition == Graph(
        nodes=[start, left, right, end],
        edges=[
            EdgeDefinition("edge_0", "start", "left", "yes"),
            EdgeDefinition("edge_1", "start", "right", "no"),
            EdgeDefinition("edge_2", "right", "end"),
            EdgeDefinition("edge_3", "left", "end"),
        ],
    )


def test_builtin_and_custom_entries_can_share_a_target() -> None:
    manual = NodeDefinition("manual", "start")
    trigger = NodeDefinition("hook", "webhook", version="2")
    end = NodeDefinition("end", "end")
    definition = (
        graph
        .GraphBuilder(entry_types=[("webhook", "2")])
        .add_root(manual)
        .add_node(end)
        .add_root(trigger)
        .connect(tail="hook", head="end")
        .build()
    )

    assert definition.entry_node_ids == {"manual", "hook"}
    assert {edge.source for edge in definition.incoming("end")} == {"manual", "hook"}
    assert definition.incoming("hook") == ()


@pytest.mark.parametrize("head", ["a", "b"])
def test_build_rejects_cycles(head: str) -> None:
    builder = (
        Graph
        .new()
        .add_root(NodeDefinition("start", "start"))
        .add_node(NodeDefinition("a", "answer"))
        .add_node(NodeDefinition("b", "answer"))
        .connect(tail="b", head=head)
    )
    with pytest.raises(GraphValidationError):
        builder.build()


def test_root_must_be_an_entry_type() -> None:
    builder = Graph.new().add_root(NodeDefinition("start", "start"))
    with pytest.raises(GraphValidationError) as exc:
        builder.add_root(NodeDefinition("answer", "answer")).build()
    assert any(issue.code == "INVALID_ROOT" for issue in exc.value.issues)


@pytest.mark.parametrize("node_id", ["", None, 1])
def test_node_definition_rejects_an_invalid_id(node_id: object) -> None:
    with pytest.raises(ValueError, match="Node ID"):
        NodeDefinition(cast(str, node_id), "start")


def test_invalid_handle_leaves_the_builder_available_for_a_retry() -> None:
    start = NodeDefinition("start", "start")
    end = NodeDefinition("end", "end")
    builder = Graph.new().add_root(start)

    with pytest.raises(TypeError, match="handle"):
        builder.add_node(end, source_handle=cast(str, None))

    builder.add_node(end)
    with pytest.raises(TypeError, match="handle"):
        builder.connect(tail="start", head="end", source_handle=cast(str, 1))
    assert builder.build() == Graph(
        nodes=[start, end], edges=[EdgeDefinition("edge_0", "start", "end")]
    )
