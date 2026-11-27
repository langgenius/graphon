"""Check host pause identities across engine events and runtime snapshots."""

import pytest
from pydantic import ValidationError

from graphon.engine_events.graph import GraphRunPausedEvent
from graphon.engine_events.node import NodeRunPauseRequestedEvent
from graphon.node_events.node import PauseRequestedEvent
from graphon.runtime import RuntimeState, VariablePool


def make_external_wait_payload(frame_id: str, execution_id: str) -> dict[str, str]:
    return {
        "TYPE": "external_wait",
        "wait_id": f"external-job:{frame_id}",
        "node_id": "worker",
        "node_title": "External worker",
        "execution_id": execution_id,
        "frame_id": frame_id,
    }


def test_external_wait_identity_survives_events_and_snapshot() -> None:
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=0
    )
    expected_payloads = []
    for frame_id in ("iteration:0", "iteration:1"):
        execution = state.graph_execution.get_or_create_node_execution(
            frame_id=frame_id, node_id="worker"
        )
        reason_payload = make_external_wait_payload(frame_id, execution.execution_id)
        pause_request = PauseRequestedEvent.model_validate({"reason": reason_payload})
        node_pause = NodeRunPauseRequestedEvent(
            id=execution.execution_id,
            node_id=reason_payload["node_id"],
            node_type="worker",
            frame_id=frame_id,
            reason=pause_request.reason,
        )
        restored_node_pause = NodeRunPauseRequestedEvent.model_validate_json(
            node_pause.model_dump_json()
        )
        assert restored_node_pause.reason.model_dump(mode="json") == reason_payload
        state.graph_execution.pause(restored_node_pause.reason)
        expected_payloads.append(reason_payload)

    graph_pause = GraphRunPausedEvent(reasons=state.graph_execution.pause_reasons)
    restored_graph_pause = GraphRunPausedEvent.model_validate_json(
        graph_pause.model_dump_json()
    )
    assert restored_graph_pause.model_dump(mode="json")["reasons"] == expected_payloads

    restored_state = RuntimeState.from_snapshot(state.dumps())
    assert restored_state.graph_execution.paused
    assert [
        reason.model_dump(mode="json")
        for reason in restored_state.graph_execution.pause_reasons
    ] == expected_payloads
    for reason_payload in expected_payloads:
        execution = restored_state.graph_execution.get_or_create_node_execution(
            frame_id=reason_payload["frame_id"], node_id=reason_payload["node_id"]
        )
        assert execution.execution_id == reason_payload["execution_id"]


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        ("wait_id", "", "string_too_short"),
        ("node_id", "", "string_too_short"),
        ("execution_id", "", "string_too_short"),
        ("frame_id", "", "string_too_short"),
        ("deadline", "host-owned", "extra_forbidden"),
    ],
)
def test_external_wait_rejects_invalid_fields(
    field: str, value: str, error_type: str
) -> None:
    reason_payload = make_external_wait_payload("root", "execution") | {field: value}
    with pytest.raises(ValidationError) as error:
        PauseRequestedEvent.model_validate({"reason": reason_payload})
    assert any(
        item["loc"][-1] == field and item["type"] == error_type
        for item in error.value.errors()
    )
