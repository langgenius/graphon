from collections.abc import Mapping
from typing import Any

import pytest

from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.file import helpers as file_helpers
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.nodes.code.code_node import CodeNode
from graphon.nodes.code.entities import CodeLanguage, CodeNodeData
from graphon.nodes.code.exc import OutputValidationError
from graphon.nodes.code.limits import CodeNodeLimits
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.variables.types import SegmentType
from tests.helpers.builders import build_graph_init_params, build_variable_pool


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


def _build_limits() -> CodeNodeLimits:
    return CodeNodeLimits(
        max_string_length=100,
        max_number=100,
        min_number=-100,
        max_precision=4,
        max_depth=5,
        max_number_array_length=10,
        max_string_array_length=10,
        max_object_array_length=10,
    )


class _CapturingExecutor:
    def __init__(self) -> None:
        self.inputs: dict[str, object] | None = None

    def execute(
        self,
        *,
        language: CodeLanguage,
        code: str,
        inputs: Mapping[str, Any],
    ) -> dict[str, Any]:
        _ = language, code
        self.inputs = dict(inputs)
        return {"result": "ok"}

    def is_execution_error(self, error: Exception) -> bool:
        _ = error
        return False


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


def test_transform_result_prioritizes_array_object_shape_errors() -> None:
    node = _build_code_node()
    output_schema = {
        "items": CodeNodeData.Output(
            type=SegmentType.ARRAY_OBJECT,
            children={"child": CodeNodeData.Output(type=SegmentType.STRING)},
        ),
    }

    with pytest.raises(
        OutputValidationError,
        match=(
            r"Output items\[1\] is not an object, got <class 'int'> "
            r"instead at index 1\."
        ),
    ):
        node.transform_result(
            result={"items": [{"child": 1}, 1]},
            output_schema=output_schema,
        )


def test_run_serializes_single_file_variable_for_code_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_resolve_file_url(_file: File, *, for_external: bool = True) -> str:
        _ = for_external
        return "https://files.example.test/report.pdf"

    monkeypatch.setattr(file_helpers, "resolve_file_url", fake_resolve_file_url)
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-file-id",
        filename="report.pdf",
        extension=".pdf",
        mime_type="application/pdf",
        size=128,
    )
    executor = _CapturingExecutor()
    node = CodeNode(
        node_id="code",
        data=CodeNodeData.model_validate({
            "type": BuiltinNodeTypes.CODE,
            "title": "Code",
            "variables": [
                {"variable": "document", "value_selector": ["start", "document"]},
            ],
            "code_language": CodeLanguage.PYTHON3,
            "code": "def main(document): return {'result': 'ok'}",
            "outputs": {"result": {"type": SegmentType.STRING}},
        }),
        graph_init_params=build_graph_init_params(),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=build_variable_pool(
                variables=[(["start", "document"], file)],
            ),
            start_at=0,
        ),
        code_executor=executor,
        code_limits=_build_limits(),
    )

    events = list(node.run())
    result = events[-1].node_run_result

    assert result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert executor.inputs == {
        "document": {
            "dify_model_identity": "__dify__file__",
            "id": None,
            "type": FileType.DOCUMENT,
            "transfer_method": FileTransferMethod.LOCAL_FILE,
            "remote_url": None,
            "reference": "upload-file-id",
            "filename": "report.pdf",
            "extension": ".pdf",
            "mime_type": "application/pdf",
            "size": 128,
            "related_id": "upload-file-id",
            "url": "https://files.example.test/report.pdf",
        },
    }
