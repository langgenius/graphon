import pytest

from graphon.nodes.code.code_node import CodeNode
from graphon.nodes.code.entities import CodeNodeData
from graphon.nodes.code.exc import OutputValidationError
from graphon.nodes.code.limits import CodeNodeLimits
from graphon.variables.types import SegmentType


def _build_code_node() -> CodeNode:
    node = object.__new__(CodeNode)
    node._limits = CodeNodeLimits(
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
        node._transform_result(result={"root": {}}, output_schema=output_schema)
