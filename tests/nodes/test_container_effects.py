import pytest
from pydantic import ValidationError

from graphon.engine.ready_queue.entities import ResumeTask
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.nodes.container_effects import (
    ContainerExecutionResult,
    ContainerNodeRunResult,
    build_container_value,
)
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import (
    ArrayAnySegment,
    ArrayBooleanSegment,
    ArrayFileSegment,
    ArrayNumberSegment,
    ArrayObjectSegment,
    ArrayStringSegment,
    FileSegment,
    IntegerSegment,
    SerializableSegment,
)
from graphon.variables.variables import ArrayFileVariable, SecretVariable
from graphon.workflow_type_encoder import WorkflowRuntimeTypeConverter


@pytest.mark.parametrize("use_builder", [False, True], ids=["direct", "builder"])
@pytest.mark.parametrize(
    "segment",
    [
        ArrayFileSegment(value=[]),
        ArrayStringSegment(value=[]),
        ArrayNumberSegment(value=[]),
        ArrayObjectSegment(value=[]),
        ArrayBooleanSegment(value=[]),
        ArrayAnySegment(value=["text"]),
        ArrayAnySegment(
            value=[
                {
                    "file": File(
                        file_type=FileType.DOCUMENT,
                        transfer_method=FileTransferMethod.LOCAL_FILE,
                        reference="example-record",
                    ),
                    "metadata": {"type": "document", "reference": "plain-value"},
                }
            ]
        ),
        ArrayFileVariable(name="files", selector=["child", "files"], value=[]),
        SecretVariable(name="token", selector=["env", "token"], value="example-value"),
    ],
    ids=[
        "file",
        "string",
        "number",
        "object",
        "boolean",
        "any",
        "nested-file",
        "variable",
        "secret",
    ],
)
def test_container_outputs_keep_their_type_after_snapshot_and_resume(
    segment: SerializableSegment,
    use_builder: bool,
) -> None:
    value = build_container_value(segment) if use_builder else segment.to_segment()
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=1
    )
    state.enqueue_ready_task(
        ResumeTask(
            invocation_id="container-run",
            result=ContainerExecutionResult(
                metadata={},
                steps=1,
                node_run_result=ContainerNodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    inputs={"input": value},
                    outputs={"output": value},
                ),
            ),
        ),
    )

    restored = RuntimeState.from_snapshot(state.dumps())
    task = restored.ready_queue.get(timeout=0.01)
    assert isinstance(task, ResumeTask)
    assert isinstance(task.result, ContainerExecutionResult)
    result = task.result.node_run_result.to_node_run_result()
    restored.variable_pool.add(("container", "output"), result.outputs["output"])

    output = restored.variable_pool.get_variable(("container", "output"))
    assert output is not None
    assert output.value_type == segment.value_type
    assert output.value == segment.value
    assert list(output.selector) == ["container", "output"]
    assert result.inputs == {"input": segment.to_object()}


def test_snapshot_distinguishes_files_from_json_in_pool_and_resume_task() -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="example-record",
    )
    file_data = file.model_dump(mode="json")
    value = [file, file_data, {"files": [file, file_data], "file_paths": [[0]]}]
    segment = ArrayAnySegment(value=value)
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=1
    )
    state.variable_pool.add(("source", "value"), segment)
    state.enqueue_ready_task(
        ResumeTask(
            invocation_id="container-run",
            result=ContainerExecutionResult(
                metadata={},
                steps=1,
                node_run_result=ContainerNodeRunResult(outputs={"output": segment}),
            ),
        )
    )

    for _ in range(2):
        state = RuntimeState.from_snapshot(state.dumps())

    stored = state.variable_pool.get_variable(("source", "value"))
    assert stored is not None
    assert stored.to_object() == value
    task = state.ready_queue.get(timeout=0.01)
    assert isinstance(task, ResumeTask)
    assert isinstance(task.result, ContainerExecutionResult)
    assert task.result.node_run_result.outputs["output"].to_object() == value


def test_nested_file_remains_downloadable_after_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    segment = ArrayAnySegment(value=[FileSegment(value=file)])
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=1
    )
    state.variable_pool.add(("source", "value"), segment)
    state.enqueue_ready_task(
        ResumeTask(
            invocation_id="container-run",
            result=ContainerExecutionResult(
                metadata={},
                steps=1,
                node_run_result=ContainerNodeRunResult(outputs={"output": segment}),
            ),
        )
    )
    for _ in range(2):
        state = RuntimeState.from_snapshot(state.dumps())

    task = state.ready_queue.get(timeout=0.01)
    assert isinstance(task, ResumeTask)
    assert isinstance(task.result, ContainerExecutionResult)
    monkeypatch.setattr(
        File, "generate_url", lambda *_args, **_kwargs: "https://example.com/download"
    )
    result = WorkflowRuntimeTypeConverter().to_json_encodable({
        "pool": state.variable_pool.get_variable(("source", "value")),
        "task": task.result.node_run_result.outputs["output"],
    })

    assert result["pool"][0]["value"]["url"] == "https://example.com/download"
    assert result["task"][0]["value"]["url"] == "https://example.com/download"


@pytest.mark.parametrize("use_builder", [False, True], ids=["direct", "builder"])
def test_live_container_outputs_rebind_variable_selectors(use_builder: bool) -> None:
    variable = ArrayFileVariable(name="files", selector=["child", "files"], value=[])
    value = build_container_value(variable) if use_builder else variable.to_segment()
    result = ContainerNodeRunResult(outputs={"files": value}).to_node_run_result()
    pool = VariablePool()

    pool.add(("container", "files"), result.outputs["files"])

    files = pool.get_variable(("container", "files"))
    assert isinstance(files, ArrayFileSegment)
    assert files.value == []
    assert list(files.selector) == ["container", "files"]


def test_container_values_preserve_secret_types_and_masking() -> None:
    secret = SecretVariable(name="token", value="example-value")

    result = ContainerNodeRunResult.model_validate({
        "inputs": {"token": secret.to_segment()},
        "outputs": {"token": build_container_value(secret)},
    }).to_node_run_result()
    pool = VariablePool()
    pool.add(("container", "token"), result.outputs["token"])

    token = pool.get(("container", "token"))
    assert token is not None
    assert token.value_type == "secret"
    assert token.value == "example-value"
    assert WorkflowRuntimeTypeConverter().to_json_encodable(result.outputs) == {
        "token": secret.log,
    }
    assert token.log == secret.log
    assert result.inputs == {"token": "example-value"}


def test_container_any_array_rejects_invalid_serialized_file() -> None:
    snapshot = ContainerNodeRunResult(
        outputs={
            "files": ArrayAnySegment(
                value=[
                    File(
                        file_type=FileType.DOCUMENT,
                        transfer_method=FileTransferMethod.LOCAL_FILE,
                        reference="example-record",
                    )
                ]
            )
        }
    ).model_dump(mode="json")
    del snapshot["outputs"]["files"]["value"][0]["reference"]
    with pytest.raises(ValidationError, match="Missing file reference"):
        ContainerNodeRunResult.model_validate(snapshot)


@pytest.mark.parametrize(
    "segment",
    [
        IntegerSegment(value=1).model_copy(update={"value": "invalid"}),
        ArrayNumberSegment(value=[1]).model_copy(update={"value": ["invalid"]}),
    ],
    ids=["integer", "array"],
)
def test_container_value_builder_rejects_invalid_typed_values(
    segment: SerializableSegment,
) -> None:
    with pytest.raises(ValidationError):
        build_container_value(segment)
