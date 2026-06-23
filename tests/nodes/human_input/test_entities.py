from typing import Any

from graphon.entities.base_node_data import BaseNodeData
from graphon.nodes.human_input.entities import HumanInputNodeData
from graphon.nodes.human_input.human_input_node import HumanInputNode

_LEGACY_HUMAN_INPUT_PAYLOAD: dict[str, Any] = {
    "type": "human-input",
    "title": "Collect Input",
    "form_content": "Name: {{#$output.name#}}",
    "inputs": [
        {
            "type": "paragraph",
            "output_variable_name": "name",
            "default": {
                "type": "constant",
                "selector": [],
                "value": "Alice",
            },
        }
    ],
    "user_actions": [
        {
            "id": "approve",
            "title": "Approve",
            "button_style": "primary",
        }
    ],
    "timeout": 3,
    "timeout_unit": "day",
}


def test_human_input_node_data_keeps_legacy_payload_fields_as_extras() -> None:
    restored = HumanInputNodeData.model_validate(_LEGACY_HUMAN_INPUT_PAYLOAD)

    assert restored.type == "human-input"
    assert restored.title == "Collect Input"
    assert restored.get("form_content") == "Name: {{#$output.name#}}"
    assert restored.get("inputs") == _LEGACY_HUMAN_INPUT_PAYLOAD["inputs"]
    assert restored.get("user_actions") == _LEGACY_HUMAN_INPUT_PAYLOAD["user_actions"]
    assert restored.get("timeout") == 3
    assert restored.get("timeout_unit") == "day"
    assert restored.get("missing", "fallback") == "fallback"


def test_human_input_node_validation_preserves_legacy_extras_from_base_node_data() -> (
    None
):
    shared_node_data = BaseNodeData.model_validate(_LEGACY_HUMAN_INPUT_PAYLOAD)

    restored = HumanInputNode.validate_node_data(shared_node_data)

    assert isinstance(restored, HumanInputNodeData)
    assert restored.title == "Collect Input"
    assert restored.get("form_content") == _LEGACY_HUMAN_INPUT_PAYLOAD["form_content"]
    assert restored.get("inputs") == _LEGACY_HUMAN_INPUT_PAYLOAD["inputs"]
    assert restored.get("user_actions") == _LEGACY_HUMAN_INPUT_PAYLOAD["user_actions"]
    assert restored.get("timeout") == _LEGACY_HUMAN_INPUT_PAYLOAD["timeout"]
    assert restored.get("timeout_unit") == _LEGACY_HUMAN_INPUT_PAYLOAD["timeout_unit"]
