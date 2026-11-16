from time import perf_counter

import pytest

from graphon.engine_events.node import NodeRunSucceededEvent
from graphon.nodes.list_operator.entities import (
    FilterBy,
    Limit,
    ListOperatorNodeData,
    OrderByConfig,
)
from graphon.nodes.list_operator.node import ListOperatorNode
from graphon.runtime.runtime_state import RuntimeState
from graphon.variables.segments import (
    ArrayBooleanSegment,
    ArrayNumberSegment,
    ArraySegment,
    ArrayStringSegment,
)

from ...helpers import build_init_params, build_variable_pool


@pytest.mark.parametrize(
    ("limit", "items", "expected"),
    [
        pytest.param(
            Limit(enabled=False, size=1),
            ArrayStringSegment(value=["alpha", "beta"]),
            ["alpha", "beta"],
            id="disabled",
        ),
        pytest.param(
            Limit(enabled=True),
            ArrayStringSegment(value=["alpha", "beta", "gamma"]),
            ["alpha", "beta", "gamma"],
            id="default",
        ),
        pytest.param(
            Limit(enabled=True, size=-2),
            ArrayNumberSegment(value=[1, 2, 3]),
            [1, 2, 3],
            id="negative",
        ),
        pytest.param(
            Limit(enabled=True, size=-10),
            ArrayBooleanSegment(value=[True, False]),
            [True, False],
            id="negative-beyond-length",
        ),
        pytest.param(
            Limit(enabled=True, size=2),
            ArrayStringSegment(value=["alpha", "beta", "gamma"]),
            ["alpha", "beta"],
            id="positive",
        ),
        pytest.param(
            Limit(enabled=True, size=5),
            ArrayNumberSegment(value=[1, 2]),
            [1, 2],
            id="beyond-length",
        ),
        pytest.param(
            Limit(enabled=True, size=0),
            ArrayBooleanSegment(value=[True, False]),
            [],
            id="zero",
        ),
        pytest.param(
            Limit(enabled=True, size=-1),
            ArrayStringSegment(value=["only"]),
            ["only"],
            id="single-element",
        ),
        pytest.param(
            Limit(enabled=True),
            ArrayNumberSegment(value=[]),
            [],
            id="empty",
        ),
    ],
)
def test_list_operator_limit(
    limit: Limit,
    items: ArraySegment,
    expected: list[str | int | bool],
) -> None:
    variable_pool = build_variable_pool(variables=[(["source", "items"], items)])
    node = ListOperatorNode(
        node_id="list_operator",
        init_params=build_init_params(),
        runtime_state=RuntimeState(
            workflow_id="workflow",
            variable_pool=variable_pool,
            start_at=perf_counter(),
        ),
        data=ListOperatorNodeData(
            title="List Operator",
            variable=["source", "items"],
            filter_by=FilterBy(),
            order_by=OrderByConfig(),
            limit=limit,
        ),
    )
    node.bind_execution_id("list-operator-run")

    events = list(node.run())
    succeeded = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    outputs = succeeded.node_run_result.outputs
    result = outputs["result"]

    assert isinstance(result, ArraySegment)
    assert result.value == expected
    assert result.value_type == items.value_type
    assert outputs["first_record"] == (expected[0] if expected else None)
    assert outputs["last_record"] == (expected[-1] if expected else None)
