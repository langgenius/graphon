import pytest

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.runtime.runtime_state import RuntimeState
from graphon.variables.variables import ObjectVariable
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


def test_node_reads_detached_variables_and_files() -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.org/original.txt",
        filename="original.txt",
    )
    pool = build_variable_pool(
        variables=[(("start", "profile"), {"tags": ["old"]}), (("start", "file"), file)]
    )
    node = _RunnableNode(
        node_id="reader",
        data=BaseNodeData(type=BuiltinNodeTypes.CODE),
        init_params=build_init_params(),
        runtime_state=RuntimeState(workflow_id="graph", variable_pool=pool, start_at=1),
    )
    node_pool = node.runtime_state.variable_pool
    profile = node_pool.get_variable(("start", "profile"))
    attachment = node_pool.get_file(("start", "file"))
    assert isinstance(profile, ObjectVariable)
    assert attachment is not None
    profile.value["tags"].append("new")
    attachment.value.filename = "changed.txt"

    stored_file = pool.get_file(("start", "file"))
    assert stored_file is not None
    assert stored_file.value.filename == "original.txt"
    assert pool.get_by_prefix("start")["profile"] == {"tags": ["old"]}
