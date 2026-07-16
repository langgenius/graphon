import pytest
from pydantic import ValidationError

from graphon.entities import GraphFailureSource
from graphon.graph_events import GraphRunFailedEvent


def test_graph_run_failed_event_carries_failure_source() -> None:
    event = GraphRunFailedEvent(
        error="upstream failed",
        exceptions_count=1,
        failure_source=GraphFailureSource(
            node_execution_id="execution-a",
            node_id="node-a",
        ),
    )

    assert event.model_dump() == {
        "error": "upstream failed",
        "exceptions_count": 1,
        "failure_source": {
            "node_execution_id": "execution-a",
            "node_id": "node-a",
        },
    }


def test_graph_failure_source_rejects_partial_payload() -> None:
    with pytest.raises(ValidationError):
        GraphFailureSource.model_validate({"node_execution_id": "execution-a"})


def test_graph_run_failed_event_defaults_to_no_failure_source() -> None:
    event = GraphRunFailedEvent(error="infrastructure failed")

    assert event.failure_source is None
