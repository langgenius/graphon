import json
from unittest.mock import MagicMock

from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.file.protocols import WorkflowFileRuntimeProtocol
from graphon.file.runtime import use_workflow_file_runtime
from graphon.nodes.container_effects import build_container_value
from graphon.variables.segments import ArrayFileSegment
from graphon.workflow_type_encoder import WorkflowRuntimeTypeConverter


def test_tuple_file_array_can_be_encoded_as_json() -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.test/file.txt",
    )
    value = build_container_value(ArrayFileSegment(value=(file,)))
    runtime = MagicMock(spec=WorkflowFileRuntimeProtocol)
    runtime.resolve_file_url.return_value = file.remote_url

    with use_workflow_file_runtime(runtime):
        result = WorkflowRuntimeTypeConverter().to_json_encodable({"files": value})

        assert json.loads(json.dumps(result)) == {"files": [file.to_dict()]}
