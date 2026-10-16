import time

import pytest

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.nodes.list_operator.entities import (
    FilterBy,
    Limit,
    ListOperatorNodeData,
    OrderByConfig,
)
from graphon.nodes.list_operator.node import ListOperatorNode
from graphon.runtime.runtime_state import RuntimeState

from ...helpers import build_init_params, build_variable_pool


def _build_list_operator_node(
    *,
    items: list[str],
    limit: Limit,
) -> ListOperatorNode:
    variable_pool = build_variable_pool(
        variables=[(["start", "items"], items)],
    )
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )
    init_params = build_init_params(graph_config={"nodes": [], "edges": []})
    node = ListOperatorNode(
        node_id="list-operator-node",
        init_params=init_params,
        runtime_state=runtime_state,
        data=ListOperatorNodeData(
            variable=["start", "items"],
            filter_by=FilterBy(enabled=False),
            order_by=OrderByConfig(enabled=False),
            limit=limit,
        ),
    )
    node.bind_execution_id("list-operator-run")
    return node


def test_limit_enabled_default_size_does_not_drop_last_element() -> None:
    node = _build_list_operator_node(
        items=["first", "second", "third"],
        limit=Limit(enabled=True),
    )

    result = node._run()

    assert result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert result.outputs["result"].value == ["first", "second", "third"]


def test_limit_enabled_positive_size_slices_correctly() -> None:
    node = _build_list_operator_node(
        items=["first", "second", "third"],
        limit=Limit(enabled=True, size=2),
    )

    result = node._run()

    assert result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert result.outputs["result"].value == ["first", "second"]


def test_limit_enabled_negative_size_raises_validation_error() -> None:
    node = _build_list_operator_node(
        items=["first", "second", "third"],
        limit=Limit(enabled=True, size=-2),
    )

    with pytest.raises(ValueError, match="Invalid limit size"):
        node._run()
