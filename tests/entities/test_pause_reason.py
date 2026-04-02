from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from graphon.entities.pause_reason import (
    HumanInputRequired,
    PauseReason,
    SchedulingPause,
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
    def test_model_validate(self, dict_value: dict[str, Any], expected: PauseReason):
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
    def test_model_construct(self, reason: PauseReason):
        holder = _Holder(reason=reason)
        assert holder.reason == reason

    def test_model_construct_with_invalid_type(self):
        with pytest.raises(ValidationError):
            _Holder(reason=object())  # type: ignore[arg-type]

    def test_unknown_type_fails_validation(self):
        with pytest.raises(ValidationError):
            _Holder.model_validate({"reason": {"TYPE": "UNKNOWN"}})
