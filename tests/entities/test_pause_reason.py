from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from graphon.entities.pause_reason import (
    HitlRequired,
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
                        "TYPE": "hitl_required",
                        "session_id": "session-1",
                        "node_id": "node_id",
                        "node_title": "node_title",
                    },
                },
                HitlRequired(
                    session_id="session-1",
                    node_id="node_id",
                    node_title="node_title",
                ),
                id="HitlRequired",
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
            HitlRequired(
                session_id="session-1",
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

    def test_hitl_required_rejects_form_payload(self) -> None:
        with pytest.raises(ValidationError):
            _Holder.model_validate({
                "reason": {
                    "TYPE": "hitl_required",
                    "session_id": "session-1",
                    "node_id": "node_id",
                    "node_title": "node_title",
                    "form_content": "form_content",
                    "inputs": [],
                    "actions": [],
                }
            })
