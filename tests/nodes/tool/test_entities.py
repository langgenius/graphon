import pytest

from graphon.nodes.tool.entities import ToolEntity, ToolNodeData


def test_tool_input_accepts_variable_selector_list() -> None:
    tool_input = ToolNodeData.ToolInput(type="variable", value=["node", "text"])

    assert tool_input.type == "variable"
    assert tool_input.value == ["node", "text"]


def test_tool_input_rejects_non_string_mixed_value() -> None:
    with pytest.raises(ValueError, match="value must be a string"):
        ToolNodeData.ToolInput(type="mixed", value=1)


def test_tool_entity_rejects_non_scalar_configuration_value() -> None:
    with pytest.raises(
        ValueError,
        match="timeout must be one of: str, int, float, bool",
    ):
        ToolEntity(
            provider_id="provider-1",
            provider_type="builtin",
            provider_name="Built-in",
            tool_name="tool",
            tool_label="Tool",
            tool_configurations={"timeout": ["10"]},
        )


def test_tool_node_filters_none_and_empty_tool_inputs() -> None:
    node_data = ToolNodeData(
        provider_id="provider-1",
        provider_type="builtin",
        provider_name="Built-in",
        tool_name="tool",
        tool_label="Tool",
        tool_configurations={"timeout": 10},
        tool_parameters={
            "answer": {"type": "mixed", "value": "ok"},
            "ignored_none": None,
            "ignored_empty": {"type": "mixed", "value": None},
        },
    )

    assert set(node_data.tool_parameters) == {"answer"}
