from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.nodes.parameter_extractor.entities import (
    ParameterConfig,
    ParameterExtractorNodeData,
)
from graphon.nodes.parameter_extractor.parameter_extractor_node import (
    ParameterExtractorNode,
)
from graphon.variables.types import SegmentType


def test_parameter_config_maps_legacy_types() -> None:
    assert (
        ParameterConfig(
            name="flag",
            type="bool",
            description="legacy bool",
            required=True,
        ).type
        == SegmentType.BOOLEAN
    )
    assert (
        ParameterConfig(
            name="choice",
            type="select",
            description="legacy select",
            required=False,
        ).type
        == SegmentType.STRING
    )


def test_parameter_extractor_node_data_builds_parameter_json_schema() -> None:
    node_data = ParameterExtractorNodeData(
        model={"provider": "test", "name": "model", "mode": LLMMode.CHAT},
        query=["start", "query"],
        parameters=[
            {
                "name": "location",
                "type": "string",
                "description": "Target location",
                "required": True,
            },
            {
                "name": "tags",
                "type": "array[string]",
                "description": "Selected tags",
                "required": False,
                "options": ["a", "b"],
            },
        ],
        reasoning_mode="function_call",
    )

    assert node_data.get_parameter_json_schema() == {
        "type": "object",
        "properties": {
            "location": {
                "description": "Target location",
                "type": "string",
            },
            "tags": {
                "description": "Selected tags",
                "type": "array",
                "items": {"type": "string"},
                "enum": ["a", "b"],
            },
        },
        "required": ["location"],
    }


def test_parameter_extractor_transform_result_uses_type_dispatch() -> None:
    node = ParameterExtractorNode.__new__(ParameterExtractorNode)
    node_data = ParameterExtractorNodeData(
        model={"provider": "test", "name": "model", "mode": LLMMode.CHAT},
        query=["start", "query"],
        parameters=[
            {
                "name": "age",
                "type": "number",
                "description": "Age",
                "required": True,
            },
            {
                "name": "enabled",
                "type": "boolean",
                "description": "Enabled",
                "required": False,
            },
            {
                "name": "name",
                "type": "string",
                "description": "Name",
                "required": False,
            },
            {
                "name": "scores",
                "type": "array[number]",
                "description": "Scores",
                "required": False,
            },
            {
                "name": "tags",
                "type": "array[string]",
                "description": "Tags",
                "required": False,
            },
            {
                "name": "payloads",
                "type": "array[object]",
                "description": "Payloads",
                "required": False,
            },
            {
                "name": "flags",
                "type": "array[boolean]",
                "description": "Flags",
                "required": False,
            },
            {
                "name": "missing_text",
                "type": "string",
                "description": "Missing text",
                "required": False,
            },
        ],
        reasoning_mode="function_call",
    )

    transformed = node._transform_result(
        node_data,
        {
            "age": "3",
            "enabled": 1,
            "name": "Alice",
            "scores": ["1", "x", 2.5],
            "tags": ["a", 2, "b"],
            "payloads": [{"ok": True}, "skip"],
            "flags": [True, 1, False],
        },
    )

    assert transformed["age"] == 3
    assert transformed["enabled"] is True
    assert transformed["name"] == "Alice"
    assert transformed["scores"].value == [1, 2.5]
    assert transformed["tags"].value == ["a", "b"]
    assert transformed["payloads"].value == [{"ok": True}]
    assert transformed["flags"].value == [True, False]
    assert not transformed["missing_text"]
