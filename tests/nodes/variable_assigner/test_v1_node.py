import time
from collections.abc import Sequence

import pytest

from graphon.engine_events.node import (
    NodeRunFailedEvent,
    NodeRunSucceededEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.nodes.variable_assigner.common import helpers as common_helpers
from graphon.nodes.variable_assigner.v1.node import VariableAssignerNode
from graphon.nodes.variable_assigner.v1.node_data import VariableAssignerData, WriteMode
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import (
    ArrayStringVariable,
    FloatVariable,
    IntegerVariable,
    StringVariable,
)

from ...helpers import build_init_params, build_variable_pool


def _build_node(
    *,
    variable_pool: VariablePool,
    assigned_selector: Sequence[str],
    write_mode: WriteMode,
    input_selector: Sequence[str],
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
        data=VariableAssignerData(
            title="Variable Assigner",
            assigned_variable_selector=assigned_selector,
            write_mode=write_mode,
            input_variable_selector=input_selector,
        ),
    )
    node.bind_execution_id("assigner-run")
    return node


def test_overwrite_string_variable() -> None:
    conversation_variable = StringVariable(
        name="test_conversation_variable",
        value="the first value",
    )
    input_variable = StringVariable(
        name="test_string_variable",
        value="the second value",
    )

    variable_pool = build_variable_pool(
        conversation_variables=[conversation_variable],
        variables=[(("node_id", input_variable.name), input_variable)],
    )
    node = _build_node(
        variable_pool=variable_pool,
        assigned_selector=("conversation", conversation_variable.name),
        write_mode=WriteMode.OVER_WRITE,
        input_selector=("node_id", input_variable.name),
    )

    events = list(node.run())
    updated_event = next(
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    succeeded_event = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    updated_variables = common_helpers.get_updated_variables(
        succeeded_event.node_run_result.process_data,
    )

    assert updated_variables is not None
    assert updated_variables[0].name == conversation_variable.name
    assert updated_variables[0].new_value == input_variable.value
    assert updated_event.variable.value == "the second value"
    assert tuple(updated_event.variable.selector) == (
        "conversation",
        conversation_variable.name,
    )
    assert succeeded_event.node_run_result.inputs == {"value": "the second value"}


def test_append_variable_to_array() -> None:
    conversation_variable = ArrayStringVariable(
        name="test_conversation_variable",
        value=["the first value"],
    )
    input_variable = StringVariable(
        name="test_string_variable",
        value="the second value",
    )

    variable_pool = build_variable_pool(
        conversation_variables=[conversation_variable],
        variables=[(("node_id", input_variable.name), input_variable)],
    )
    node = _build_node(
        variable_pool=variable_pool,
        assigned_selector=("conversation", conversation_variable.name),
        write_mode=WriteMode.APPEND,
        input_selector=("node_id", input_variable.name),
    )

    events = list(node.run())
    updated_event = next(
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    succeeded_event = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    updated_variables = common_helpers.get_updated_variables(
        succeeded_event.node_run_result.process_data,
    )

    assert updated_variables is not None
    assert updated_variables[0].name == conversation_variable.name
    assert updated_variables[0].new_value == ["the first value", "the second value"]
    assert updated_event.variable.value == ["the first value", "the second value"]


def test_clear_array() -> None:
    conversation_variable = ArrayStringVariable(
        name="test_conversation_variable",
        value=["the first value"],
    )

    variable_pool = build_variable_pool(conversation_variables=[conversation_variable])
    node = _build_node(
        variable_pool=variable_pool,
        assigned_selector=("conversation", conversation_variable.name),
        write_mode=WriteMode.CLEAR,
        input_selector=(),
    )

    events = list(node.run())
    updated_event = next(
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    succeeded_event = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    updated_variables = common_helpers.get_updated_variables(
        succeeded_event.node_run_result.process_data,
    )

    assert updated_variables is not None
    assert updated_variables[0].new_value == []
    assert updated_event.variable.value == []
    assert succeeded_event.node_run_result.inputs == {"value": []}


@pytest.mark.parametrize(
    ("original", "write_mode", "input_value"),
    [
        (
            IntegerVariable(name="target", value=1),
            WriteMode.OVER_WRITE,
            "not a number",
        ),
        (
            ArrayStringVariable(name="target", value=["first"]),
            WriteMode.APPEND,
            42,
        ),
    ],
    ids=["invalid-overwrite", "invalid-append"],
)
def test_invalid_value_fails_without_updating_variables(
    original: IntegerVariable | ArrayStringVariable,
    write_mode: WriteMode,
    input_value: str | int,
) -> None:
    pool = build_variable_pool(
        conversation_variables=[original],
        variables=[(("input", "value"), input_value)],
    )
    original_before = original.model_copy(deep=True)
    pool_before = pool.model_copy(deep=True)
    node = _build_node(
        variable_pool=pool,
        assigned_selector=("conversation", original.name),
        write_mode=write_mode,
        input_selector=("input", "value"),
    )

    events = list(node.run())

    assert any(isinstance(event, NodeRunFailedEvent) for event in events)
    assert not any(isinstance(event, NodeRunVariableUpdatedEvent) for event in events)
    assert original == original_before
    assert pool == pool_before


def test_overwrite_number_keeps_fractional_value_and_variable_identity() -> None:
    original = IntegerVariable(
        id="number-id",
        name="number",
        description="Mutable number",
        selector=["conversation", "number"],
        value=1,
    )
    pool = build_variable_pool(
        conversation_variables=[original],
        variables=[(("input", "number"), FloatVariable(name="input", value=0.5))],
    )
    node = _build_node(
        variable_pool=pool,
        assigned_selector=original.selector,
        write_mode=WriteMode.OVER_WRITE,
        input_selector=("input", "number"),
    )

    update = next(
        event for event in node.run() if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    restored = NodeRunVariableUpdatedEvent.model_validate_json(update.model_dump_json())

    assert restored.variable.value == 0.5  # ruff: ignore[float-equality-comparison] - Exact binary fraction.
    assert restored.variable.value_type == "float"
    assert restored.variable.id == original.id
    assert restored.variable.name == original.name
    assert restored.variable.description == original.description
    assert restored.variable.selector == original.selector
