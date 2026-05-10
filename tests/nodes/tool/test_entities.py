import pytest

from graphon.nodes.tool.entities import (
    ToolEntity,
    ToolInputType,
    ToolNodeData,
    ToolProviderType,
)


def test_tool_input_accepts_variable_selector_list() -> None:
    tool_input = ToolNodeData.ToolInput(
        type=ToolInputType.VARIABLE,
        value=["node", "text"],
    )

    assert tool_input.type == ToolInputType.VARIABLE
    assert tool_input.value == ["node", "text"]


def test_tool_input_rejects_non_string_mixed_value() -> None:
    with pytest.raises(ValueError, match="value must be a string"):
        ToolNodeData.ToolInput(type=ToolInputType.MIXED, value=1)


def test_tool_entity_rejects_non_scalar_configuration_value() -> None:
    with pytest.raises(
        ValueError,
        match="timeout must be one of: str, int, float, bool, dict",
    ):
        ToolEntity.model_validate({
            "provider_id": "provider-1",
            "provider_type": ToolProviderType.BUILT_IN,
            "provider_name": "Built-in",
            "tool_name": "tool",
            "tool_label": "Tool",
            "tool_configurations": {"timeout": ["10"]},
        })


def test_tool_node_filters_none_and_empty_tool_inputs() -> None:
    node_data = ToolNodeData.model_validate({
        "provider_id": "provider-1",
        "provider_type": ToolProviderType.BUILT_IN,
        "provider_name": "Built-in",
        "tool_name": "tool",
        "tool_label": "Tool",
        "tool_configurations": {"timeout": 10},
        "tool_parameters": {
            "answer": {"type": ToolInputType.MIXED, "value": "ok"},
            "ignored_none": None,
            "ignored_empty": {"type": ToolInputType.MIXED, "value": None},
        },
    })

    assert set(node_data.tool_parameters) == {"answer"}
