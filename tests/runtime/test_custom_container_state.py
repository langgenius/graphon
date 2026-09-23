import json
from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter, ValidationError

from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    ContainerNodeRunResult,
    ContainerRunResult,
    CustomContainerRequest,
)
from graphon.runtime.container_state import (
    CustomContainerFrameState,
    CustomContainerRunState,
    FrameRuntimeData,
    create_container_run_state,
)
from graphon.runtime.ready_queue import ResumeTask
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool


def test_create_custom_container_run_state() -> None:
    started_at = datetime.now(UTC).replace(tzinfo=None)
    request = TypeAdapter(ContainerAwaitRequest).validate_python({
        "kind": "custom",
        "payload": '{"version":1,"graph":"workflow-1"}',
    })
    assert isinstance(request, CustomContainerRequest)

    run_state = create_container_run_state(
        invocation_id="invocation-1",
        frame_id="root",
        node_id="workflow-tool",
        started_at=started_at,
        request=request,
    )

    assert run_state == CustomContainerRunState(
        invocation_id="invocation-1",
        frame_id="root",
        node_id="workflow-tool",
        started_at=started_at,
        payload=request.payload,
    )


def test_custom_container_state_round_trips_in_runtime_snapshot() -> None:
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=1
    )
    run_state = CustomContainerRunState(
        invocation_id="invocation-1",
        frame_id="root",
        node_id="workflow-tool",
        started_at=datetime.now(UTC).replace(tzinfo=None),
        payload='{"version":1,"graph":"workflow-1"}',
    )
    frame_state = CustomContainerFrameState(
        frame_id="workflow-tool-frame",
        parent_invocation_id=run_state.invocation_id,
        runtime_data=FrameRuntimeData(
            variable_pool=VariablePool(),
            outputs={"result": "done"},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=2,
            graph_node_states={},
            graph_edge_states={},
        ),
    )
    state.put_container_run(run_state)
    state.put_container_frame(frame_state)

    restored = RuntimeState.from_snapshot(state.dumps())

    assert restored.get_container_run(run_state.invocation_id) == run_state
    assert restored.get_container_frame(frame_state.frame_id) == frame_state


def test_custom_container_request_is_not_a_container_run_result() -> None:
    request = CustomContainerRequest(payload="{}")

    with pytest.raises(ValidationError):
        TypeAdapter(ContainerRunResult).validate_python(request.model_dump())


def test_legacy_pending_result_does_not_recount_its_saved_child() -> None:
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=1
    )
    run = CustomContainerRunState(
        invocation_id="invocation",
        frame_id="root",
        node_id="tool",
        started_at=datetime.now(UTC).replace(tzinfo=None),
        payload="{}",
    )
    state.put_container_run(run)
    state.put_container_frame(
        CustomContainerFrameState(
            frame_id="child",
            parent_invocation_id=run.invocation_id,
            runtime_data=FrameRuntimeData(
                variable_pool=VariablePool(),
                outputs={},
                llm_usage=LLMUsage.from_metadata({"total_tokens": 50}),
                node_run_steps=2,
                graph_node_states={},
                graph_edge_states={},
            ),
        )
    )
    state.ready_queue.put(
        ResumeTask(
            invocation_id=run.invocation_id,
            result=ContainerExecutionResult(
                metadata={},
                steps=3,
                node_run_result=ContainerNodeRunResult(
                    status="succeeded",
                    llm_usage=LLMUsage.from_metadata({"total_tokens": 70}),
                ),
            ),
        )
    )
    legacy = json.loads(state.dumps())
    legacy["version"] = "3.0"

    restored = RuntimeState.from_snapshot(json.dumps(legacy))

    assert (restored.node_run_steps, restored.total_tokens) == (2, 70)
