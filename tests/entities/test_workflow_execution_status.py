from graphon.enums import WorkflowExecutionStatus


class TestWorkflowExecutionStatus:
    def test_is_ended_method(self):
        ended_statuses = [
            WorkflowExecutionStatus.SUCCEEDED,
            WorkflowExecutionStatus.FAILED,
            WorkflowExecutionStatus.PARTIAL_SUCCEEDED,
            WorkflowExecutionStatus.STOPPED,
        ]

        for status in ended_statuses:
            assert status.is_ended(), f"{status} should be considered ended"

        non_ended_statuses = [
            WorkflowExecutionStatus.SCHEDULED,
            WorkflowExecutionStatus.RUNNING,
            WorkflowExecutionStatus.PAUSED,
        ]

        for status in non_ended_statuses:
            assert not status.is_ended(), f"{status} should not be considered ended"

    def test_ended_values(self):
        assert set(WorkflowExecutionStatus.ended_values()) == frozenset((
            WorkflowExecutionStatus.SUCCEEDED.value,
            WorkflowExecutionStatus.FAILED.value,
            WorkflowExecutionStatus.PARTIAL_SUCCEEDED.value,
            WorkflowExecutionStatus.STOPPED.value,
        ))
