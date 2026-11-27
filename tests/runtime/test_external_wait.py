"""Check host pause identities across engine events and runtime snapshots."""

import pytest
from pydantic import ValidationError

from graphon.engine_events.graph import GraphRunPausedEvent
from graphon.engine_events.node import NodeRunPauseRequestedEvent
from graphon.node_events.node import PauseRequestedEvent
from graphon.runtime import RuntimeState, VariablePool


def pause_reason(frame_id: str, execution_id: str) -> dict[str, str]:
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
    expected = []
    for frame_id in ("iteration:0", "iteration:1"):
        execution = state.graph_execution.get_or_create_node_execution(
            frame_id=frame_id, node_id="worker"
        )
        reason = pause_reason(frame_id, execution.execution_id)
        request = PauseRequestedEvent.model_validate({"reason": reason})
        node_paused = NodeRunPauseRequestedEvent(
            id=execution.execution_id,
            node_id=reason["node_id"],
            node_type="worker",
            reason=request.reason,
        )
        restored_pause = NodeRunPauseRequestedEvent.model_validate_json(
            node_paused.model_dump_json()
        )
        assert restored_pause.reason.model_dump(mode="json") == reason
        state.graph_execution.pause(restored_pause.reason)
        expected.append(reason)

    paused = GraphRunPausedEvent(reasons=state.graph_execution.pause_reasons)
    restored_event = GraphRunPausedEvent.model_validate_json(paused.model_dump_json())
    assert restored_event.model_dump(mode="json")["reasons"] == expected

    restored = RuntimeState.from_snapshot(state.dumps())
    assert restored.graph_execution.paused
    assert [
        reason.model_dump(mode="json")
        for reason in restored.graph_execution.pause_reasons
    ] == expected
    for reason in expected:
        execution = restored.graph_execution.get_or_create_node_execution(
            frame_id=reason["frame_id"], node_id=reason["node_id"]
        )
        assert execution.execution_id == reason["execution_id"]


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
    reason = pause_reason("root", "execution") | {field: value}
    with pytest.raises(ValidationError) as error:
        PauseRequestedEvent.model_validate({"reason": reason})
    assert any(
        item["loc"][-1] == field and item["type"] == error_type
        for item in error.value.errors()
    )
