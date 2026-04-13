import pytest

from graphon.nodes.code.code_node import CodeNode
from graphon.nodes.code.entities import CodeNodeData
from graphon.nodes.code.exc import OutputValidationError
from graphon.nodes.code.limits import CodeNodeLimits
from graphon.variables.types import SegmentType


def _build_code_node() -> CodeNode:
    node = object.__new__(CodeNode)
    node.limits = CodeNodeLimits(
        max_string_length=100,
        max_number=100,
        min_number=-100,
        max_precision=4,
        max_depth=5,
        max_number_array_length=10,
        max_string_array_length=10,
        max_object_array_length=10,
    )
    return node


def test_transform_result_reports_nested_missing_field_without_leading_dot() -> None:
    node = _build_code_node()
    output_schema = {
        "root": CodeNodeData.Output(
            type=SegmentType.OBJECT,
            children={"child": CodeNodeData.Output(type=SegmentType.STRING)},
        ),
    }

    with pytest.raises(OutputValidationError, match=r"Output root\.child is missing\."):
        node.transform_result(result={"root": {}}, output_schema=output_schema)


def test_transform_result_rejects_non_string_array_elements_with_validation_error() -> (
    None
):
    node = _build_code_node()
    output_schema = {
        "items": CodeNodeData.Output(type=SegmentType.ARRAY_STRING),
    }

    with pytest.raises(
        OutputValidationError,
        match=r"Output items\[1\] must be a string, got int instead\.",
    ):
        node.transform_result(
            result={"items": ["valid", 1]},
            output_schema=output_schema,
        )
