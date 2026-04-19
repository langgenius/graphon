from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from graphon.entities.pause_reason import (
    HumanInputRequired,
    PauseReason,
    SchedulingPause,
)
from graphon.nodes.human_input.enums import (
    FormInputType,
    PlaceholderType,
)


class _Holder(BaseModel):
    reason: PauseReason


class TestPauseReasonDiscriminator:
    @pytest.mark.parametrize(
        ("dict_value", "expected"),
        [
            pytest.param(
                {
                    "reason": {
                        "TYPE": "human_input_required",
                        "form_id": "form_id",
                        "form_content": "form_content",
                        "node_id": "node_id",
                        "node_title": "node_title",
                    },
                },
                HumanInputRequired(
                    form_id="form_id",
                    form_content="form_content",
                    node_id="node_id",
                    node_title="node_title",
                ),
                id="HumanInputRequired",
            ),
            pytest.param(
                {
                    "reason": {
                        "TYPE": "scheduled_pause",
                        "message": "Hold on",
                    },
                },
                SchedulingPause(message="Hold on"),
                id="SchedulingPause",
            ),
        ],
    )
    def test_model_validate(
        self, dict_value: dict[str, Any], expected: PauseReason
    ) -> None:
        holder = _Holder.model_validate(dict_value)

        assert type(holder.reason) is type(expected)
        assert holder.reason == expected

    @pytest.mark.parametrize(
        "reason",
        [
            HumanInputRequired(
                form_id="form_id",
                form_content="form_content",
                node_id="node_id",
                node_title="node_title",
            ),
            SchedulingPause(message="Hold on"),
        ],
        ids=lambda x: type(x).__name__,
    )
    def test_model_construct(self, reason: PauseReason) -> None:
        holder = _Holder(reason=reason)
        assert holder.reason == reason

    def test_model_validate_with_invalid_type(self) -> None:
        with pytest.raises(ValidationError):
            _Holder.model_validate({"reason": object()})

    def test_unknown_type_fails_validation(self) -> None:
        with pytest.raises(ValidationError):
            _Holder.model_validate({"reason": {"TYPE": "UNKNOWN"}})

    def test_human_input_required_model_validate_accepts_current_form_input_payload(
        self,
    ):

        form_inputs_json = [
            {
                "type": "text_input",
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

        actions_json = [
            {
                "id": "approve",
                "title": "Approve",
                "button_style": "primary",
            }
        ]
        payload = {
            "reason": {
                "TYPE": "human_input_required",
                "form_id": "form_id",
                "form_content": "form_content",
                "inputs": form_inputs_json,
                "actions": actions_json,
                "node_id": "node_id",
                "node_title": "node_title",
                "resolved_default_values": {"name": "Alice"},
            }
        }

        restored = _Holder.model_validate(payload)

        assert isinstance(restored.reason, HumanInputRequired)
        assert restored.reason.form_id == "form_id"
        assert restored.reason.form_content == "form_content"
        assert restored.reason.node_id == "node_id"
        assert restored.reason.node_title == "node_title"
        assert len(restored.reason.inputs) == 2
        assert restored.reason.inputs[0].type == FormInputType.TEXT_INPUT
        assert restored.reason.inputs[0].output_variable_name == "name"
        assert restored.reason.inputs[0].default is not None
        assert restored.reason.inputs[0].default.type == PlaceholderType.CONSTANT
        assert restored.reason.inputs[0].default.value == "Alice"
        assert restored.reason.inputs[1].type == FormInputType.PARAGRAPH
        assert restored.reason.inputs[1].default is not None
        assert restored.reason.inputs[1].default.type == PlaceholderType.VARIABLE
        assert restored.reason.inputs[1].default.selector == ["start", "bio"]
        assert restored.reason.inputs[1].default.value == ""
        assert [action.id for action in restored.reason.actions] == ["approve"]
        assert restored.reason.actions[0].button_style.value == "primary"
        assert restored.reason.resolved_default_values == {"name": "Alice"}
