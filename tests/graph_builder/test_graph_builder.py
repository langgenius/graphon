from typing import cast

import pytest

from graphon.graph import EdgeDefinition, Graph, GraphReference, NodeDefinition
from graphon.graph.validation import GraphValidationError
from graphon.graph_builder import GraphBuilder


def test_build_a_branch_and_join_with_explicit_edges() -> None:
    builder = GraphBuilder()
    start_id = builder.add_node("start", "start")
    start_only = builder.build()
    end_id = builder.add_node("end", "end", config={"outputs": []})
    for node_id, handle in [("left", "yes"), ("right", "no")]:
        branch_id = builder.add_node(node_id, "answer", config={"answer": node_id})
        builder.connect(start_id, branch_id, source_handle=handle)
        builder.connect(branch_id, end_id)

    assert start_only == Graph(nodes=[NodeDefinition("start", "start")])
    assert builder.build() == Graph(
        nodes=[
            NodeDefinition("start", "start"),
            NodeDefinition("left", "answer", config={"answer": "left"}),
            NodeDefinition("right", "answer", config={"answer": "right"}),
            NodeDefinition("end", "end", config={"outputs": []}),
        ],
        edges=[
            EdgeDefinition("edge_0", "start", "left", "yes"),
            EdgeDefinition("edge_1", "left", "end"),
            EdgeDefinition("edge_2", "start", "right", "no"),
            EdgeDefinition("edge_3", "right", "end"),
        ],
    )


def test_builtin_and_custom_entries_can_share_a_target() -> None:
    builder = GraphBuilder(entry_types=[("webhook", "2")])
    end_id = builder.add_node("end", "end")
    manual_id = builder.add_node("manual", "start")
    trigger_id = builder.add_node("hook", "webhook", version="2")
    builder.connect(manual_id, end_id)
    builder.connect(trigger_id, end_id)
    definition = builder.build()

    assert definition.entry_node_ids == {"manual", "hook"}
    assert {edge.source for edge in definition.incoming("end")} == {"manual", "hook"}
    assert definition.incoming("hook") == ()


def test_container_nodes_keep_embedded_and_referenced_graphs() -> None:
    body = Graph(nodes=[NodeDefinition("enter", "iteration-start")])
    reference = GraphReference("body", version="v2")
    builder = GraphBuilder()
    start_id = builder.add_node("start", "start")
    for node_id, call_target in [("inline", body), ("external", reference)]:
        iteration_id = builder.add_node(node_id, "iteration", call_target=call_target)
        builder.connect(start_id, iteration_id)
    graph = builder.build()

    assert graph.node("inline").call_target == body
    assert graph.node("external").call_target == reference


@pytest.mark.parametrize("cycle_target_id", ["a", "b"])
def test_build_rejects_cycles(cycle_target_id: str) -> None:
    builder = GraphBuilder()
    start_id = builder.add_node("start", "start")
    first_answer_id = builder.add_node("a", "answer")
    second_answer_id = builder.add_node("b", "answer")
    builder.connect(start_id, first_answer_id)
    builder.connect(first_answer_id, second_answer_id)
    builder.connect(second_answer_id, cycle_target_id)
    with pytest.raises(GraphValidationError):
        builder.build()


def test_build_requires_an_entry() -> None:
    builder = GraphBuilder()
    builder.add_node("reply", "answer")
    with pytest.raises(GraphValidationError) as exc:
        builder.build()
    assert any(issue.code == "INVALID_ROOT" for issue in exc.value.issues)


def test_rejected_edits_leave_the_builder_available_for_a_retry() -> None:
    builder = GraphBuilder()
    start_id = builder.add_node("start", "start")
    end_id = builder.add_node("end", "end")

    with pytest.raises(GraphValidationError, match="DUPLICATE_NODE_ID"):
        builder.add_node("end", "answer")
    with pytest.raises(ValueError, match="missing"):
        builder.connect(start_id, "missing")
    with pytest.raises(TypeError, match="handle"):
        builder.connect(start_id, end_id, source_handle=cast(str, None))

    builder.connect(start_id, end_id)
    assert builder.build() == Graph(
        nodes=[NodeDefinition("start", "start"), NodeDefinition("end", "end")],
        edges=[EdgeDefinition("edge_0", "start", "end")],
    )
