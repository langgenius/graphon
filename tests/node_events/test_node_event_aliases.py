from graphon.graph_events.node import (
    NodeRunPauseRequestedEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.node_events.base import NodeRunResult
from graphon.node_events.node import PauseRequestedEvent, VariableUpdatedEvent


def test_variable_alias_still_validates_in_event_models() -> None:
    payload = {
        "variable": {
            "value_type": "string",
            "value": "hello",
            "name": "greeting",
            "selector": ["start", "greeting"],
        },
    }

    node_event = VariableUpdatedEvent.model_validate(payload)
    graph_event = NodeRunVariableUpdatedEvent.model_validate({
        **payload,
        "id": "evt-1",
        "node_id": "start",
        "node_type": "start",
        "node_run_result": NodeRunResult(status="succeeded").model_dump(mode="python"),
    })

    assert node_event.variable.value == "hello"
    assert graph_event.variable.selector == ["start", "greeting"]


def test_pause_reason_alias_still_validates_in_event_models() -> None:
    payload = {"reason": {"TYPE": "scheduled_pause", "message": "Hold on"}}

    node_event = PauseRequestedEvent.model_validate(payload)
    graph_event = NodeRunPauseRequestedEvent.model_validate({
        **payload,
        "id": "evt-2",
        "node_id": "start",
        "node_type": "start",
        "node_run_result": NodeRunResult(status="succeeded").model_dump(mode="python"),
    })

    assert node_event.reason.message == "Hold on"
    assert graph_event.reason.message == "Hold on"
