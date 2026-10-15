import pytest

from graphon.engine.ready_queue.entities import ResumeTask
from graphon.enums import WorkflowNodeExecutionStatus
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
    SerializableSegment,
)
from graphon.variables.variables import ArrayFileVariable, SecretVariable


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
        ArrayFileVariable(name="files", selector=["child", "files"], value=[]),
    ],
    ids=["file", "string", "number", "object", "boolean", "any", "variable"],
)
def test_container_outputs_keep_their_type_after_snapshot_and_resume(
    segment: SerializableSegment,
    use_builder: bool,
) -> None:
    value = build_container_value(segment) if use_builder else segment
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


@pytest.mark.parametrize("use_builder", [False, True], ids=["direct", "builder"])
def test_live_container_outputs_rebind_variable_selectors(use_builder: bool) -> None:
    variable = ArrayFileVariable(name="files", selector=["child", "files"], value=[])
    value = build_container_value(variable) if use_builder else variable
    result = ContainerNodeRunResult(outputs={"files": value}).to_node_run_result()
    pool = VariablePool()

    pool.add(("container", "files"), result.outputs["files"])

    files = pool.get_variable(("container", "files"))
    assert isinstance(files, ArrayFileSegment)
    assert files.value == []
    assert list(files.selector) == ["container", "files"]


def test_container_values_keep_existing_secret_normalization() -> None:
    secret = SecretVariable(name="token", value="example-value")

    result = ContainerNodeRunResult.model_validate({
        "inputs": {"token": secret},
        "outputs": {"token": build_container_value(secret)},
    }).to_node_run_result()
    pool = VariablePool()
    pool.add(("container", "token"), result.outputs["token"])

    token = pool.get(("container", "token"))
    assert token is not None
    assert token.value_type == "string"
    assert token.value == "example-value"
    assert result.inputs == {"token": "example-value"}
