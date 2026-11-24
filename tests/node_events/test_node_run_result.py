from graphon.enums import WorkflowNodeExecutionMetadataKey, WorkflowNodeExecutionStatus
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult


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


def test_node_run_result_shares_immutable_empty_llm_usage() -> None:
    first = NodeRunResult()
    second = NodeRunResult()

    assert first.llm_usage is second.llm_usage
    assert first.llm_usage is LLMUsage.empty_usage()
    assert first.metadata is not second.metadata
