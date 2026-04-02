from graphon.nodes.variable_assigner.v2.enums import Operation
from graphon.nodes.variable_assigner.v2.helpers import is_input_value_valid
from graphon.variables.types import SegmentType


def test_is_input_value_valid_overwrite_array_string():
    assert is_input_value_valid(
        variable_type=SegmentType.ARRAY_STRING,
        operation=Operation.OVER_WRITE,
        value=["hello", "world"],
    )
    assert is_input_value_valid(
        variable_type=SegmentType.ARRAY_STRING,
        operation=Operation.OVER_WRITE,
        value=[],
    )

    assert not is_input_value_valid(
        variable_type=SegmentType.ARRAY_STRING,
        operation=Operation.OVER_WRITE,
        value="not an array",
    )
    assert not is_input_value_valid(
        variable_type=SegmentType.ARRAY_STRING,
        operation=Operation.OVER_WRITE,
        value=[1, 2, 3],
    )
    assert not is_input_value_valid(
        variable_type=SegmentType.ARRAY_STRING,
        operation=Operation.OVER_WRITE,
        value=["valid", 123, "invalid"],
    )


def test_is_input_value_valid_rejects_divide_by_zero():
    assert not is_input_value_valid(
        variable_type=SegmentType.NUMBER,
        operation=Operation.DIVIDE,
        value=0,
    )


def test_is_input_value_valid_accepts_clear_without_input():
    assert is_input_value_valid(
        variable_type=SegmentType.ARRAY_OBJECT,
        operation=Operation.CLEAR,
        value=None,
    )


def test_is_input_value_valid_accepts_array_any_append_string() -> None:
    assert is_input_value_valid(
        variable_type=SegmentType.ARRAY_ANY,
        operation=Operation.APPEND,
        value="hello",
    )
