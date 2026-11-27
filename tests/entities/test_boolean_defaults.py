"""Exercise Graphon's node error defaults without changing 0.8 compatibility."""

import pytest
from pydantic import ValidationError

from graphon.entities.base_node_data import DefaultValue


def test_boolean_node_default_accepts_false() -> None:
    value = DefaultValue.model_validate({
        "type": "boolean",
        "key": "accepted",
        "value": False,
    }).value
    assert value is False


def test_boolean_array_default_accepts_only_boolean_elements() -> None:
    value = DefaultValue.model_validate({
        "type": "array[boolean]",
        "key": "flags",
        "value": [True, False],
    }).value
    assert len(value) == 2
    assert value[0] is True
    assert value[1] is False
    with pytest.raises(ValidationError, match="All elements must be bool"):
        DefaultValue.model_validate({
            "type": "array[boolean]",
            "key": "flags",
            "value": [1, False],
        })


def test_scalar_boolean_node_default_does_not_coerce_text() -> None:
    with pytest.raises(ValidationError, match="must be bool"):
        DefaultValue.model_validate({
            "type": "boolean",
            "key": "accepted",
            "value": "false",
        })


def test_number_default_still_converts_a_numeric_string() -> None:
    value = DefaultValue.model_validate({
        "type": "number",
        "key": "legacy",
        "value": "2.5",
    }).value
    assert value == pytest.approx(2.5)
