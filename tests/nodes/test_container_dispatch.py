from collections.abc import Generator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionMetadataKey
from graphon.graph_events.graph import GraphRunSucceededEvent
from graphon.graph_events.node import NodeRunStartedEvent, NodeRunSucceededEvent
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.loop import LoopFailedEvent, LoopSucceededEvent
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.iteration.entities import ErrorHandleMode, IterationNodeData
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.entities import LoopNodeData
from graphon.nodes.loop.loop_node import LoopNode
from graphon.variables.segments import StringSegment
from graphon.variables.variables import IntegerVariable


def test_iteration_node_single_iter_keeps_iteration_event_dispatch() -> None:
    node = IterationNode.__new__(IterationNode)
    node.init_node_identity("iteration-node")
    node.init_node_data({
        "type": "iteration",
        "iterator_selector": ["input", "items"],
        "output_selector": ["iteration-node", "answer"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
    })

    variable_pool = MagicMock()
    variable_pool.get.side_effect = lambda selector: {
        ("iteration-node", "index"): IntegerVariable(
            name="index",
            selector=["iteration-node", "index"],
            value=2,
        ),
        ("iteration-node", "answer"): StringSegment(value="done"),
    }.get(tuple(selector))

    child_event = NodeRunStartedEvent(
        id="child-run-1",
        node_id="child-node",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Code",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    graph_engine = SimpleNamespace(
        run=lambda: iter([child_event, GraphRunSucceededEvent()]),
    )
    outputs: list[object] = []

    yielded_events = list(
        node.run_single_iter(
            variable_pool=variable_pool,
            outputs=outputs,
            graph_engine=graph_engine,
        ),
    )

    assert yielded_events == [child_event]
    assert child_event.in_iteration_id == "iteration-node"
    assert (
        child_event.node_run_result.metadata[
            WorkflowNodeExecutionMetadataKey.ITERATION_INDEX
        ]
        == 2
    )
    assert outputs == ["done"]


def test_loop_node_single_loop_keeps_loop_end_dispatch() -> None:
    node = LoopNode.__new__(LoopNode)
    node.init_node_identity("loop-node")
    node.init_node_data({
        "type": "loop",
        "loop_count": 1,
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    node.graph_runtime_state = SimpleNamespace(variable_pool=MagicMock())

    loop_end_event = NodeRunSucceededEvent(
        id="loop-end-1",
        node_id="loop-end-node",
        node_type=BuiltinNodeTypes.LOOP_END,
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    graph_engine = SimpleNamespace(run=lambda: iter([loop_end_event]))
    loop_state: dict[str, bool] = {}

    yielded_events = list(
        node.run_single_loop(
            graph_engine=graph_engine,
            current_index=1,
            loop_state=loop_state,
        ),
    )

    assert yielded_events == [loop_end_event]
    assert loop_end_event.in_loop_id == "loop-node"
    assert loop_state["reach_break_node"] is True
    assert node.node_data.outputs["loop_round"] == 2


def test_iteration_variable_mapping_filters_container_internal_selectors() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "iteration",
                "data": {
                    "type": "iteration",
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


def _build_loop_node_for_run(loop_count: int = 3) -> LoopNode:
    node = LoopNode.__new__(LoopNode)
    node.init_node_identity("loop-node")
    node.init_node_data({
        "type": "loop",
        "loop_count": loop_count,
        "break_conditions": [],
        "logical_operator": "and",
        "outputs": {},
    })
    node.graph_runtime_state = SimpleNamespace(llm_usage=LLMUsage.empty_usage())

    def initialize_loop_run(
        *,
        inputs: dict[str, object],
    ) -> tuple[str, dict[str, list[str]], set[str]]:
        _ = inputs
        return "start", {}, set()

    cast(Any, node)._initialize_loop_run = initialize_loop_run
    return node


def test_loop_run_initial_break_reports_zero_steps() -> None:
    node = _build_loop_node_for_run(loop_count=3)
    cast(Any, node)._evaluate_break_conditions = lambda **_kwargs: True

    events = list(node._run())

    success_event = next(
        event for event in events if isinstance(event, LoopSucceededEvent)
    )
    assert success_event.steps == 0
    assert (
        success_event.metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON]
        == "loop_break"
    )


def test_loop_run_mid_break_keeps_original_step_count() -> None:
    node = _build_loop_node_for_run(loop_count=3)
    break_results = iter([False, True])
    cast(Any, node)._evaluate_break_conditions = lambda **_kwargs: next(
        break_results,
    )

    def execute_iteration(**kwargs: Any) -> Generator[object, None, None]:
        iteration_state = cast(dict[str, Any], kwargs["iteration_state"])
        iteration_state["iteration_usage"] = LLMUsage.empty_usage()
        iteration_state["reach_break_node"] = False
        iteration_state["loop_duration"] = 0.1
        iteration_state["single_loop_variable"] = {"acc": "value"}
        yield from ()

    cast(Any, node)._execute_loop_iteration = execute_iteration

    events = list(node._run())

    success_event = next(
        event for event in events if isinstance(event, LoopSucceededEvent)
    )
    assert success_event.steps == 3
    assert success_event.metadata[
        WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP
    ] == {"0": 0.1}
    assert success_event.metadata[
        WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP
    ] == {"0": {"acc": "value"}}


def test_loop_run_failure_keeps_accumulated_state() -> None:
    node = _build_loop_node_for_run(loop_count=2)
    cast(Any, node)._evaluate_break_conditions = lambda **_kwargs: False
    usage_one = LLMUsage.empty_usage().model_copy(update={"total_tokens": 1})
    usage_two = LLMUsage.empty_usage().model_copy(update={"total_tokens": 2})

    def execute_iteration(**kwargs: Any) -> Generator[object, None, None]:
        current_index = cast(int, kwargs["current_index"])
        iteration_state = cast(dict[str, Any], kwargs["iteration_state"])
        if current_index == 0:
            iteration_state["iteration_usage"] = usage_one
            iteration_state["reach_break_node"] = False
            iteration_state["loop_duration"] = 0.1
            iteration_state["single_loop_variable"] = {"acc": "first"}
            yield from ()
            return

        iteration_state["iteration_usage"] = usage_two
        msg = "child failed"
        raise RuntimeError(msg)
        yield

    cast(Any, node)._execute_loop_iteration = execute_iteration

    events = list(node._run())

    failed_event = next(event for event in events if isinstance(event, LoopFailedEvent))
    assert failed_event.steps == 2
    assert failed_event.metadata[
        WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP
    ] == {"0": 0.1}
    assert failed_event.metadata[
        WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP
    ] == {"0": {"acc": "first"}}
    assert failed_event.metadata[WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS] == 3
