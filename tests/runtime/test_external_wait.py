"""Check host-owned wait data across engine events and runtime snapshots."""

import pytest
from pydantic import ValidationError

from graphon.engine_events.graph import GraphRunPausedEvent
from graphon.engine_events.node import NodeRunPauseRequestedEvent
from graphon.entities.pause_reason import ExternalWait
from graphon.node_events.node import PauseRequestedEvent
from graphon.runtime import RuntimeState, VariablePool


@pytest.fixture
def reason_payload() -> dict[str, str]:
    return {
        "TYPE": "external_wait",
        "wait_type": "dify.hitl",
        "wait_id": "form-1",
        "node_id": "worker",
        "node_title": "",
        "node_execution_id": "execution-1",
        "frame_id": "root",
    }


@pytest.mark.parametrize(
    ("wait_type", "wait_id"),
    [
        ("hitl", ""),
        ("dify.hitl", "session:1"),
        ("dify.hitl.v1", "  审核 / request\n"),
        ("langgenius.scheduling_pause", "job:1"),
        ("_owner.Custom_2", "job:2"),
    ],
)
def test_external_wait_preserves_opaque_id(
    reason_payload: dict[str, str], wait_type: str, wait_id: str
) -> None:
    reason_payload.update(wait_type=wait_type, wait_id=wait_id)
    pause_request = PauseRequestedEvent.model_validate({"reason": reason_payload})

    assert isinstance(pause_request.reason, ExternalWait)
    assert pause_request.reason.model_dump(mode="json") == reason_payload


@pytest.mark.parametrize(
    "wait_type",
    [
        "",
        "dify.",
        ".dify",
        "dify..hitl",
        "dify.1hitl",
        "dify.h-itl",
        "dify.人",
        "dify.hitl\n",
    ],
)
def test_external_wait_type_requires_dot_separated_ascii_identifiers(
    reason_payload: dict[str, str], wait_type: str
) -> None:
    reason_payload["wait_type"] = wait_type
    with pytest.raises(ValidationError) as error:
        PauseRequestedEvent.model_validate({"reason": reason_payload})

    assert {issue["loc"][-1] for issue in error.value.errors()} == {"wait_type"}


@pytest.mark.parametrize(
    "field_name",
    ["wait_type", "wait_id", "node_id", "node_title", "node_execution_id", "frame_id"],
)
def test_external_wait_requires_field(
    reason_payload: dict[str, str], field_name: str
) -> None:
    del reason_payload[field_name]

    with pytest.raises(ValidationError) as error:
        PauseRequestedEvent.model_validate({"reason": reason_payload})

    assert any(
        issue["loc"][-1] == field_name and issue["type"] == "missing"
        for issue in error.value.errors()
    )


@pytest.mark.parametrize("field_name", ["node_id", "node_execution_id", "frame_id"])
def test_external_wait_rejects_empty_identity(
    reason_payload: dict[str, str], field_name: str
) -> None:
    reason_payload[field_name] = ""
    with pytest.raises(ValidationError) as error:
        PauseRequestedEvent.model_validate({"reason": reason_payload})

    assert any(
        issue["loc"][-1] == field_name and issue["type"] == "string_too_short"
        for issue in error.value.errors()
    )


def test_external_wait_round_trips_in_pause_events_and_runtime_snapshot() -> None:
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=0
    )
    expected_reason_payloads: list[dict[str, str]] = []
    expected_execution_ids: dict[str, str] = {}
    for index, wait_type in enumerate(("dify.external_agent", "dify.hitl")):
        frame_id = f"iteration:{index}"
        execution = state.graph_execution.get_or_create_node_execution(
            frame_id=frame_id, node_id="worker"
        )
        reason_payload = {
            "TYPE": "external_wait",
            "wait_type": wait_type,
            "wait_id": "opaque: / 等待",
            "node_id": "worker",
            "node_title": "Worker",
            "node_execution_id": execution.execution_id,
            "frame_id": frame_id,
        }
        pause_request = PauseRequestedEvent.model_validate({"reason": reason_payload})
        node_pause = NodeRunPauseRequestedEvent(
            id=execution.execution_id,
            node_id="worker",
            node_type="worker",
            frame_id=frame_id,
            reason=pause_request.reason,
        )
        restored_node_pause = NodeRunPauseRequestedEvent.model_validate_json(
            node_pause.model_dump_json()
        )
        assert restored_node_pause.reason.model_dump(mode="json") == reason_payload
        assert restored_node_pause.frame_id == frame_id
        assert restored_node_pause.id == execution.execution_id
        state.graph_execution.pause(restored_node_pause.reason)
        expected_reason_payloads.append(reason_payload)
        expected_execution_ids[frame_id] = execution.execution_id

    graph_pause = GraphRunPausedEvent(reasons=state.graph_execution.pause_reasons)
    restored_graph_pause = GraphRunPausedEvent.model_validate_json(
        graph_pause.model_dump_json()
    )
    assert (
        restored_graph_pause.model_dump(mode="json")["reasons"]
        == expected_reason_payloads
    )

    restored_state = RuntimeState.from_snapshot(state.dumps())
    assert restored_state.graph_execution.paused
    assert [
        reason.model_dump(mode="json")
        for reason in restored_state.graph_execution.pause_reasons
    ] == expected_reason_payloads
    for frame_id, execution_id in expected_execution_ids.items():
        restored_execution = (
            restored_state.graph_execution.get_or_create_node_execution(
                frame_id=frame_id, node_id="worker"
            )
        )
        assert restored_execution.execution_id == execution_id
