import pytest

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.runtime.runtime_state import RuntimeState
from tests.helpers import build_init_params, build_variable_pool


class _RunnableNode(Node[BaseNodeData]):
    node_type = BuiltinNodeTypes.CODE

    @classmethod
    def version(cls) -> str:
        return "1"

    def _run(self) -> NodeRunResult:
        return NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED)


def test_node_run_requires_bound_execution_id() -> None:
    node = _RunnableNode(
        node_id="node",
        data=BaseNodeData(type=BuiltinNodeTypes.CODE),
        init_params=build_init_params(),
        runtime_state=RuntimeState(
            workflow_id="workflow",
            variable_pool=build_variable_pool(),
            start_at=1,
        ),
    )

    with pytest.raises(RuntimeError, match="execution_id must be bound"):
        list(node.run())
