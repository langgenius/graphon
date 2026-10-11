import json

import pytest
from pydantic import ValidationError

from graphon.entities.base_node_data import BaseNodeData


@pytest.mark.parametrize(
    ("value_type", "value", "expected"),
    [
        ("boolean", False, False),
        ("boolean", True, True),
        ("boolean", "false", False),
        ("boolean", "true", True),
        ("array[boolean]", [], []),
        ("array[boolean]", [False, True], [False, True]),
        ("array[boolean]", "[]", []),
        ("array[boolean]", "[false, true]", [False, True]),
    ],
)
def test_boolean_default_values_round_trip(
    value_type: str,
    value: object,
    expected: object,
) -> None:
    payload = {
        "type": "code",
        "error_strategy": "default-value",
        "default_value": [{"key": "result", "type": value_type, "value": value}],
    }

    data = BaseNodeData.model_validate_json(json.dumps(payload))
    restored = BaseNodeData.model_validate_json(data.model_dump_json())

    for parsed in (data, restored):
        result = parsed.default_value_dict["result"]
        assert result == expected
        assert type(result) is type(expected)
        if isinstance(result, list):
            assert all(isinstance(item, bool) for item in result)


@pytest.mark.parametrize(
    ("value_type", "value"),
    [
        ("boolean", 0),
        ("boolean", 1),
        ("boolean", '"false"'),
        ("boolean", None),
        ("boolean", ""),
        ("array[boolean]", [1]),
        ("array[boolean]", '["false"]'),
        ("array[boolean]", "[true, 0]"),
        ("array[boolean]", None),
        ("array[boolean]", ""),
    ],
)
def test_boolean_default_values_reject_wrong_value_types(
    value_type: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        BaseNodeData.model_validate({
            "type": "code",
            "default_value": [{"key": "result", "type": value_type, "value": value}],
        })


@pytest.mark.parametrize(
    ("value_type", "value", "expected"),
    [
        ("string", "", ""),
        ("number", "0", 0),
        ("array[string]", '["fallback"]', ["fallback"]),
    ],
)
def test_existing_default_values_still_parse(
    value_type: str,
    value: object,
    expected: object,
) -> None:
    data = BaseNodeData.model_validate({
        "type": "code",
        "default_value": [{"key": "result", "type": value_type, "value": value}],
    })

    assert data.default_value_dict == {"result": expected}
