from copy import deepcopy
from typing import Any

import pytest

from graphon.model_runtime.v2 import DataFormat, JsonObject, JsonValue


def test_data_format_owns_schema_and_preserves_open_metadata() -> None:
    examples: list[JsonValue] = [None, False, 3, 1.25, "text", {"tags": ["initial"]}]
    schema: JsonObject = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {"value": {"type": "string"}},
        "$ref": "#/$defs/value",
        "examples": examples,
    }
    expected_schema = deepcopy(schema)
    data_format = DataFormat(
        schema=schema, kinds=("vendor:future", "json"), profile="vendor:profile"
    )
    schema["$ref"] = "#/changed"
    source_items: Any = examples
    source_items[-1]["tags"].append("source mutation")
    assert data_format.schema == expected_schema
    copied_examples: Any = data_format.schema["examples"]
    assert tuple(map(type, copied_examples[:5])) == (type(None), bool, int, float, str)
    returned_schema: Any = data_format.schema
    returned_schema["examples"][-1]["tags"].append("read mutation")
    returned_schema["new"] = True
    assert data_format.schema == expected_schema
    assert data_format.kinds == ("vendor:future", "json")
    assert data_format.profile == "vendor:profile"
    for field, value in (("schema", {}), ("kinds", ()), ("profile", None)):
        with pytest.raises(AttributeError):
            setattr(data_format, field, value)


def test_data_format_accepts_shared_json_without_treating_it_as_a_cycle() -> None:
    shared: JsonObject = {"items": [1, "two"]}
    schema: JsonObject = {
        "type": "vendor:future",
        "format": 42,
        "left": shared,
        "right": shared,
    }
    data_format = DataFormat(schema=schema, kinds=())
    assert data_format.schema == schema
    assert data_format.profile is None


@pytest.mark.parametrize("schema", [None, True, 1, 1.0, "object", []])
def test_data_format_requires_an_object_schema(schema: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        DataFormat(schema=schema, kinds=())


@pytest.mark.parametrize(
    "value",
    [
        (),
        {1},
        b"text",
        object(),
        float("nan"),
        float("inf"),
        -float("inf"),
        {1: "value"},
    ],
)
def test_data_format_rejects_non_json_values_without_coercion(value: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        DataFormat(schema={"nested": [value]}, kinds=())


@pytest.mark.parametrize("container_type", [list, dict])
def test_data_format_rejects_json_cycles(container_type: Any) -> None:
    value = container_type()
    if isinstance(value, list):
        value.append(value)
    else:
        value["self"] = value
    with pytest.raises((TypeError, ValueError)):
        DataFormat(schema={"nested": value}, kinds=())
