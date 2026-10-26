from graphon.enums import WorkflowNodeExecutionMetadataKey, WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeRunResult


def test_node_run_result_without_attachments_remains_readable() -> None:
    result = NodeRunResult.model_validate({"outputs": {"text": "hello"}})

    assert result.outputs == {"text": "hello"}
    assert result.attachments == []


def test_node_run_result_accepts_trigger_info_metadata() -> None:
    result = NodeRunResult(
        status=WorkflowNodeExecutionStatus.SUCCEEDED,
        metadata={
            WorkflowNodeExecutionMetadataKey.TRIGGER_INFO: {
                "provider_id": "provider-id",
                "event_name": "event-name",
            },
        },
    )

    assert result.metadata[WorkflowNodeExecutionMetadataKey.TRIGGER_INFO] == {
        "provider_id": "provider-id",
        "event_name": "event-name",
    }
