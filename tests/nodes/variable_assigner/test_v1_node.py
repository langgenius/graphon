import time
from collections.abc import Sequence

from graphon.graph_events.node import NodeRunSucceededEvent, NodeRunVariableUpdatedEvent
from graphon.nodes.variable_assigner.common import helpers as common_helpers
from graphon.nodes.variable_assigner.v1.node import VariableAssignerNode
from graphon.nodes.variable_assigner.v1.node_data import VariableAssignerData, WriteMode
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
    assigned_selector: Sequence[str],
    write_mode: WriteMode,
    input_selector: Sequence[str],
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
        config=VariableAssignerData(
            title="Variable Assigner",
            assigned_variable_selector=assigned_selector,
            write_mode=write_mode,
            input_variable_selector=input_selector,
        ),
    )


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
