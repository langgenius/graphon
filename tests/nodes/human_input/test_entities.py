from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from graphon.nodes.base.entities import VariableSelector
from graphon.nodes.human_input.entities import (
    FormDefinition,
    HumanInputNodeData,
    ParagraphInput,
    StringSource,
)
from graphon.nodes.human_input.enums import (
    FormInputType,
    TimeoutUnit,
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
    form_input: ParagraphInput


class TestHumanInputNodeDataDeserialization:
    def test_model_validate_accepts_current_form_input_payload(self) -> None:
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
        assert restored.form_content == "Name: {{#$output.name#}}"
        assert len(restored.inputs) == 2
        assert isinstance(restored.inputs[0], ParagraphInput)
        assert restored.inputs[0].type == FormInputType.PARAGRAPH
        assert restored.inputs[0].output_variable_name == "name"
        assert restored.inputs[0].default is not None
        assert restored.inputs[0].default.type == ValueSourceType.CONSTANT
        assert restored.inputs[0].default.selector == []
        assert restored.inputs[0].default.value == "Alice"

        assert isinstance(restored.inputs[1], ParagraphInput)

        assert restored.inputs[1].type == FormInputType.PARAGRAPH
        assert restored.inputs[1].default is not None
        assert restored.inputs[1].default.type == ValueSourceType.VARIABLE
        assert restored.inputs[1].default.selector == ["start", "bio"]
        assert [action.id for action in restored.user_actions] == ["approve", "reject"]
        assert [action.button_style.value for action in restored.user_actions] == [
            "primary",
            "ghost",
        ]
        assert restored.timeout == 3
        assert restored.timeout_unit == TimeoutUnit.DAY


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

        assert isinstance(restored.inputs[0], ParagraphInput)
        assert restored.inputs[0].type == FormInputType.PARAGRAPH
        assert restored.inputs[0].default is not None
        assert restored.inputs[0].default.type == ValueSourceType.CONSTANT
        assert restored.inputs[0].default.value == "Alice"

        assert isinstance(restored.inputs[1], ParagraphInput)
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
            form_input=ParagraphInput(
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


class TestHumanInputNodeDataVariableSelectorMapping:
    def test_extract_variable_mapping_preserves_current_paragraph_input_behavior(
        self,
    ) -> None:
        node_data = HumanInputNodeData(
            title="Collect Input",
            form_content=(
                "Profile: {{#start.user.name#}} "
                "Query: {{#sys.query#}} "
                "Output: {{#$output.answer#}}"
            ),
            inputs=[
                ParagraphInput(
                    output_variable_name="notes",
                ),
                ParagraphInput(
                    output_variable_name="summary",
                    default=StringSource(
                        type=ValueSourceType.CONSTANT,
                        value="Pinned summary",
                    ),
                ),
                ParagraphInput(
                    output_variable_name="bio",
                    default=StringSource(
                        type=ValueSourceType.VARIABLE,
                        selector=("input", "profile", "bio"),
                    ),
                ),
            ],
        )

        mapping = node_data.extract_variable_selector_to_variable_mapping("human-node")

        assert mapping == {
            "human-node.#start.user#": ["start", "user"],
            "human-node.#sys.query#": ["sys", "query"],
            "human-node.#input.profile.bio#": ("input", "profile", "bio"),
        }

    def test_extract_variable_mapping_ignores_short_template_selectors(
        self,
        monkeypatch: Any,
    ) -> None:
        def _extract_short_selector(_self: Any) -> list[VariableSelector]:
            return [
                VariableSelector(
                    variable="#start#",
                    value_selector=["start"],
                )
            ]

        monkeypatch.setattr(
            "graphon.nodes.human_input.entities.VariableTemplateParser.extract_variable_selectors",
            _extract_short_selector,
        )

        node_data = HumanInputNodeData(
            title="Collect Input",
            form_content="ignored",
            inputs=[],
        )

        mapping = node_data.extract_variable_selector_to_variable_mapping("human-node")

        assert mapping == {}
