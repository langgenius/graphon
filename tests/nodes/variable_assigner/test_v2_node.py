import time
from collections.abc import Sequence

import pytest

from graphon.engine_events.node import (
    NodeRunFailedEvent,
    NodeRunSucceededEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.nodes.variable_assigner.v2.entities import (
    VariableAssignerNodeData,
    VariableOperationItem,
)
from graphon.nodes.variable_assigner.v2.enums import InputType, Operation
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import (
    ArrayFileVariable,
    ArrayStringVariable,
    IntegerVariable,
    StringVariable,
)

from ...helpers import build_init_params, build_variable_pool


def _build_node(
    *,
    variable_pool: VariablePool,
    items: Sequence[VariableOperationItem],
) -> VariableAssignerNode:
    graph_config = {"nodes": [], "edges": []}
    init_params = build_init_params(graph_config=graph_config)
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )

    node = VariableAssignerNode(
        node_id="assigner",
        init_params=init_params,
        runtime_state=runtime_state,
        data=VariableAssignerNodeData(
            title="Variable Assigner",
            version="2",
            items=items,
        ),
    )
    node.bind_execution_id("assigner-run")
    return node


def test_remove_first_from_array() -> None:
    conversation_variable = ArrayStringVariable(
        name="test_conversation_variable",
        value=["first", "second", "third"],
        selector=["conversation", "test_conversation_variable"],
    )

    variable_pool = build_variable_pool(conversation_variables=[conversation_variable])
    node = _build_node(
        variable_pool=variable_pool,
        items=[
            VariableOperationItem(
                variable_selector=["conversation", conversation_variable.name],
                input_type=InputType.VARIABLE,
                operation=Operation.REMOVE_FIRST,
                value=None,
            ),
        ],
    )

    events = list(node.run())

    updated_event = next(
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    assert updated_event.variable.value == ["second", "third"]


def test_remove_last_from_array() -> None:
    conversation_variable = ArrayStringVariable(
        name="test_conversation_variable",
        value=["first", "second", "third"],
        selector=["conversation", "test_conversation_variable"],
    )

    variable_pool = build_variable_pool(conversation_variables=[conversation_variable])
    node = _build_node(
        variable_pool=variable_pool,
        items=[
            VariableOperationItem(
                variable_selector=["conversation", conversation_variable.name],
                input_type=InputType.VARIABLE,
                operation=Operation.REMOVE_LAST,
                value=None,
            ),
        ],
    )

    events = list(node.run())

    updated_event = next(
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    assert updated_event.variable.value == ["first", "second"]


def test_clear_preserves_the_file_array_type() -> None:
    variable = ArrayFileVariable(
        name="files", selector=["iteration", "output"], value=[]
    )
    pool = build_variable_pool(variables=[(variable.selector, variable)])
    node = _build_node(
        variable_pool=pool,
        items=[
            VariableOperationItem(
                variable_selector=variable.selector,
                input_type=InputType.VARIABLE,
                operation=Operation.CLEAR,
            ),
        ],
    )

    updates = [
        event for event in node.run() if isinstance(event, NodeRunVariableUpdatedEvent)
    ]

    assert len(updates) == 1
    assert updates[0].variable.value_type == "array[file]"
    assert updates[0].variable.value == []
    assert updates[0].variable.selector == variable.selector


def test_multiple_operations_emit_single_final_update_per_selector() -> None:
    conversation_variable = ArrayStringVariable(
        name="test_conversation_variable",
        value=["first"],
        selector=["conversation", "test_conversation_variable"],
    )
    second = StringVariable(name="second", value="second")
    third = StringVariable(name="third", value="third")

    variable_pool = build_variable_pool(
        conversation_variables=[conversation_variable],
        variables=[(("inputs", "second"), second), (("inputs", "third"), third)],
    )
    node = _build_node(
        variable_pool=variable_pool,
        items=[
            VariableOperationItem(
                variable_selector=["conversation", conversation_variable.name],
                input_type=InputType.VARIABLE,
                operation=Operation.APPEND,
                value=["inputs", "second"],
            ),
            VariableOperationItem(
                variable_selector=["conversation", conversation_variable.name],
                input_type=InputType.VARIABLE,
                operation=Operation.APPEND,
                value=["inputs", "third"],
            ),
        ],
    )

    events = list(node.run())

    update_events = [
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    ]
    succeeded_event = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )

    assert len(update_events) == 1
    assert update_events[0].variable.value == ["first", "second", "third"]
    assert (
        succeeded_event.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    )
    assert succeeded_event.node_run_result.process_data[
        "test_conversation_variable"
    ] == ["first", "second", "third"]


def test_invalid_constant_input_returns_failed_event() -> None:
    conversation_variable = StringVariable(
        name="test_conversation_variable",
        value="before",
        selector=["conversation", "test_conversation_variable"],
    )

    variable_pool = build_variable_pool(conversation_variables=[conversation_variable])
    node = _build_node(
        variable_pool=variable_pool,
        items=[
            VariableOperationItem(
                variable_selector=["conversation", conversation_variable.name],
                input_type=InputType.CONSTANT,
                operation=Operation.OVER_WRITE,
                value=123,
            ),
        ],
    )

    events = list(node.run())

    failed_event = next(
        event for event in events if isinstance(event, NodeRunFailedEvent)
    )
    assert failed_event.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    assert failed_event.node_run_result.error == "Invalid input value 123"


@pytest.mark.parametrize(
    ("operation", "value", "expected_value"),
    [
        (Operation.DIVIDE, 2, 0.5),
        (Operation.ADD, 0.5, 1.5),
    ],
)
def test_numeric_updates_keep_valid_types_and_variable_identity(
    operation: Operation,
    value: float,
    expected_value: float,
) -> None:
    original = IntegerVariable(
        id="number-id",
        name="number",
        description="Mutable number",
        selector=["conversation", "number"],
        value=1,
    )
    pool = build_variable_pool(conversation_variables=[original])
    node = _build_node(
        variable_pool=pool,
        items=[
            VariableOperationItem(
                variable_selector=original.selector,
                input_type=InputType.CONSTANT,
                operation=operation,
                value=value,
            ),
        ],
    )

    update = next(
        event for event in node.run() if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    restored = NodeRunVariableUpdatedEvent.model_validate_json(update.model_dump_json())

    assert restored.variable.value == expected_value
    assert restored.variable.value_type == "float"
    assert restored.variable.id == original.id
    assert restored.variable.name == original.name
    assert restored.variable.description == original.description
    assert restored.variable.selector == original.selector
