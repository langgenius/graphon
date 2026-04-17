import time
from collections.abc import Sequence

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunSucceededEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.nodes.variable_assigner.v2.entities import (
    VariableAssignerNodeData,
    VariableOperationItem,
)
from graphon.nodes.variable_assigner.v2.enums import InputType, Operation
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import (
    ArrayStringVariable,
    StringVariable,
)

from ...helpers import build_graph_init_params, build_variable_pool


def _build_node(
    *,
    variable_pool: VariablePool,
    items: Sequence[VariableOperationItem],
) -> VariableAssignerNode:
    graph_config = {"nodes": [], "edges": []}
    init_params = build_graph_init_params(graph_config=graph_config)
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )

    return VariableAssignerNode(
        node_id="assigner",
        graph_init_params=init_params,
        graph_runtime_state=runtime_state,
        data=VariableAssignerNodeData(
            title="Variable Assigner",
            version="2",
            items=items,
        ),
    )


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
