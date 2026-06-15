from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from graphon.nodes.human_input.entities import (
    FormDefinition,
    HumanInputNodeData,
    ParagraphInputConfig,
    StringSource,
    UserActionConfig,
)
from graphon.nodes.human_input.enums import (
    ButtonStyle,
    FormInputType,
    ValueSourceType,
)

_FORM_INPUTS_JSON_PAYLOAD = [
    {
        "type": "paragraph",
        "output_variable_name": "name",
        "default": {
            "type": "constant",
            "selector": [],
            "value": "Alice",
        },
    },
    {
        "type": "paragraph",
        "output_variable_name": "bio",
        "default": {
            "type": "variable",
            "selector": ["start", "bio"],
            "value": "",
        },
    },
]

_USER_ACTIONS_JSON_PAYLOAD = [
    {
        "id": "approve",
        "title": "Approve",
        "button_style": "primary",
    },
    {
        "id": "reject",
        "title": "Reject",
        "button_style": "ghost",
    },
]


class _FormInputHolder(BaseModel):
    form_input: ParagraphInputConfig


class TestHumanInputNodeDataDeserialization:
    def test_model_validate_keeps_legacy_form_payload_as_extras(self) -> None:
        payload: dict[str, Any] = {
            "type": "human-input",
            "title": "Collect Input",
            "form_content": "Name: {{#$output.name#}}",
            "inputs": _FORM_INPUTS_JSON_PAYLOAD,
            "user_actions": _USER_ACTIONS_JSON_PAYLOAD,
            "timeout": 3,
            "timeout_unit": "day",
        }

        restored = HumanInputNodeData.model_validate(payload)

        assert restored.type == "human-input"
        assert restored.title == "Collect Input"
        assert restored.get("form_content") == "Name: {{#$output.name#}}"
        assert restored.get("inputs") == _FORM_INPUTS_JSON_PAYLOAD
        assert restored.get("user_actions") == _USER_ACTIONS_JSON_PAYLOAD
        assert restored.get("timeout") == 3
        assert restored.get("timeout_unit") == "day"


class TestFormDefinitionDeserialization:
    def test_model_validate_accepts_current_form_input_payload(self) -> None:
        payload: dict[str, Any] = {
            "form_content": "Name: {{#$output.name#}}",
            "inputs": _FORM_INPUTS_JSON_PAYLOAD,
            "user_actions": _USER_ACTIONS_JSON_PAYLOAD,
            "rendered_content": "Name: Alice",
            "expiration_time": "2026-04-19T12:00:00Z",
            "default_values": {"bio": "Graph runtime"},
            "node_title": "Collect Input",
            "display_in_ui": True,
        }

        restored = FormDefinition.model_validate(payload)

        assert restored.form_content == "Name: {{#$output.name#}}"
        assert restored.rendered_content == "Name: Alice"
        assert len(restored.inputs) == 2

        assert isinstance(restored.inputs[0], ParagraphInputConfig)
        assert restored.inputs[0].type == FormInputType.PARAGRAPH
        assert restored.inputs[0].default is not None
        assert restored.inputs[0].default.type == ValueSourceType.CONSTANT
        assert restored.inputs[0].default.value == "Alice"

        assert isinstance(restored.inputs[1], ParagraphInputConfig)
        assert restored.inputs[1].type == FormInputType.PARAGRAPH
        assert restored.inputs[1].default is not None
        assert restored.inputs[1].default.selector == ["start", "bio"]
        assert [action.id for action in restored.user_actions] == ["approve", "reject"]
        assert restored.default_values == {"bio": "Graph runtime"}
        assert restored.node_title == "Collect Input"
        assert restored.display_in_ui is True
        assert restored.expiration_time == datetime(2026, 4, 19, 12, 0, tzinfo=UTC)


class TestFormInputRoundTrip:
    def test_paragraph_roundtrip_in_wrapper_model(self) -> None:
        original = _FormInputHolder(
            form_input=ParagraphInputConfig(
                type=FormInputType.PARAGRAPH,
                output_variable_name="bio",
                default=StringSource(
                    type=ValueSourceType.VARIABLE,
                    selector=("start", "bio"),
                ),
            )
        )

        payload = original.model_dump(mode="json")
        restored = _FormInputHolder.model_validate(payload)

        assert payload == {
            "form_input": {
                "type": "paragraph",
                "output_variable_name": "bio",
                "default": {
                    "type": "variable",
                    "selector": ["start", "bio"],
                    "value": "",
                },
            }
        }
        assert restored.form_input.type == FormInputType.PARAGRAPH
        assert restored.form_input.output_variable_name == "bio"
        assert restored.form_input.default is not None
        assert restored.form_input.default.type == ValueSourceType.VARIABLE
        assert restored.form_input.default.selector == ["start", "bio"]
        assert restored.form_input.default.value == ""


def test_user_action_title_accepts_long_business_value() -> None:
    action = UserActionConfig(
        id="approve",
        title="card_visa_enterprise_001_long_value",
        button_style=ButtonStyle.DEFAULT,
    )

    assert action.title == "card_visa_enterprise_001_long_value"
