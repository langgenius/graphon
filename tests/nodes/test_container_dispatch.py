from graphon.enums import ErrorHandleMode
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.container_effects import (
    IterationFrameRequest,
    LoopFrameRequest,
    build_container_value,
)
from graphon.nodes.iteration.entities import IterationNodeData
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.entities import LoopNodeData
from graphon.nodes.loop.loop_node import LoopNode


def test_container_await_requests_have_intrinsic_kind_tags() -> None:
    loop_request = LoopFrameRequest(
        inputs={"loop_count": build_container_value(1)},
        outputs={},
        loop_count=1,
        root_node_id="loop-start",
        loop_variable_selectors={},
        loop_node_ids=frozenset(),
        index=0,
    )
    iteration_request = IterationFrameRequest(
        items=(build_container_value("a"),),
        root_node_id="iteration-start",
        indexes=(0,),
        output_selector=("answer", "text"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=1,
    )

    assert loop_request.kind == "loop"
    assert iteration_request.kind == "iteration"


def test_iteration_variable_mapping_filters_container_internal_selectors() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
                    "start_node_id": "iteration-start",
                    "iterator_selector": ["input", "items"],
                    "output_selector": ["child", "answer"],
                },
            },
            {
                "id": "child",
                "data": {
                    "type": AnswerNode.node_type,
                    "iteration_id": "iteration",
                    "answer": (
                        "{{#source.value#}} {{#iteration.item#}} {{#nested.answer#}}"
                    ),
                },
            },
            {
                "id": "nested",
                "data": {
                    "type": AnswerNode.node_type,
                    "iteration_id": "iteration",
                    "answer": "{{#source.other#}}",
                },
            },
        ],
    }

    mapping = IterationNode._extract_variable_selector_to_variable_mapping(
        graph_config=graph_config,
        node_id="iteration",
        node_data=IterationNodeData.model_validate({
            "type": "iteration",
            "start_node_id": "iteration-start",
            "iterator_selector": ["input", "items"],
            "output_selector": ["child", "answer"],
        }),
    )

    assert mapping == {
        "iteration.input_selector": ["input", "items"],
        "child.child.#source.value#": ["source", "value"],
        "nested.nested.#source.other#": ["source", "other"],
    }


def test_loop_variable_mapping_filters_loop_internal_selectors() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "loop",
                "data": {
                    "type": "loop",
                    "start_node_id": "loop-start",
                    "loop_count": 2,
                    "break_conditions": [],
                    "logical_operator": "and",
                },
            },
            {
                "id": "child",
                "data": {
                    "type": AnswerNode.node_type,
                    "loop_id": "loop",
                    "answer": "{{#source.value#}} {{#loop.acc#}}",
                },
            },
        ],
    }

    mapping = LoopNode._extract_variable_selector_to_variable_mapping(
        graph_config=graph_config,
        node_id="loop",
        node_data=LoopNodeData.model_validate({
            "type": "loop",
            "start_node_id": "loop-start",
            "loop_count": 2,
            "break_conditions": [],
            "logical_operator": "and",
            "loop_variables": [
                {
                    "label": "acc",
                    "var_type": "string",
                    "value_type": "variable",
                    "value": ["start", "seed"],
                },
            ],
        }),
    )

    assert mapping == {
        "child.child.#source.value#": ["source", "value"],
        "loop.acc": ["start", "seed"],
    }
