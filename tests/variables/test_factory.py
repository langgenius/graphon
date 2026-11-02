from typing import assert_type

import pytest
from pydantic import TypeAdapter

from graphon.variables.factory import (
    TypeMismatchError,
    build_segment,
    build_segment_with_type,
)
from graphon.variables.segments import SerializableSegment, StringSegment
from graphon.variables.types import SegmentType


@pytest.mark.parametrize(
    ("value_type", "value"),
    [
        (SegmentType.ARRAY_ANY, ["text"]),
        (SegmentType.ARRAY_FILE, []),
        (SegmentType.FLOAT, 1),
        (SegmentType.SECRET, "example-value"),
    ],
)
def test_declared_type_accepts_compatible_values(
    value_type: SegmentType, value: object
) -> None:
    segment = build_segment_with_type(value_type, value)

    assert segment.value_type == value_type
    assert segment.value == value


@pytest.mark.parametrize(
    ("value_type", "value"),
    [(SegmentType.INTEGER, "1"), (SegmentType.ARRAY_STRING, [1])],
)
def test_declared_type_rejects_incompatible_values(
    value_type: SegmentType, value: object
) -> None:
    with pytest.raises(TypeMismatchError):
        build_segment_with_type(value_type, value)


def test_empty_array_inference_is_consistent() -> None:
    assert SegmentType.infer_segment_type([]) == build_segment([]).value_type


def test_serialized_segment_tag_selects_its_value_type() -> None:
    segment = TypeAdapter(SerializableSegment).validate_json(
        '{"value_type": "string", "value": "text"}'
    )

    if segment.value_type == SegmentType.STRING:
        assert_type(segment, StringSegment)
        assert_type(segment.value, str)
        assert segment.value == "text"
    else:
        pytest.fail("String tag did not select a string segment")
