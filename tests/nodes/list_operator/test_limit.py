"""Regression coverage for the List Operator node's ``limit`` operation."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from graphon.engine_events.node import NodeRunSucceededEvent
from graphon.nodes.list_operator.entities import (
    ExtractConfig,
    FilterBy,
    Limit,
    ListOperatorNodeData,
    OrderByConfig,
)
from graphon.nodes.list_operator.node import ListOperatorNode
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool

from ...helpers import build_init_params, build_variable_pool

_ITEMS = ["alpha", "beta", "gamma", "delta"]


def _run_limit(
    *,
    size: int,
    enabled: bool = True,
    values: Sequence[str] | None = None,
) -> list[str]:
    variable_pool: VariablePool = build_variable_pool(
        variables=[(["source", "items"], list(_ITEMS if values is None else values))],
    )
    init_params = build_init_params()
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=variable_pool,
        start_at=time.perf_counter(),
    )
    node = ListOperatorNode(
        node_id="list_operator",
        init_params=init_params,
        runtime_state=runtime_state,
        data=ListOperatorNodeData(
            title="List Operator",
            variable=["source", "items"],
            filter_by=FilterBy(),
            order_by=OrderByConfig(),
            limit=Limit(enabled=enabled, size=size),
            extract_by=ExtractConfig(),
        ),
    )
    node.bind_execution_id("list-operator-run")
    succeeded = next(
        event for event in node.run() if isinstance(event, NodeRunSucceededEvent)
    )
    result: Any = succeeded.node_run_result.outputs["result"]
    return list(result.value)


def test_limit_disabled_keeps_every_element() -> None:
    assert _run_limit(size=-1, enabled=False) == _ITEMS


def test_limit_default_size_keeps_every_element() -> None:
    """The unset ``Limit.size`` default of -1 must not drop the last element."""
    assert Limit().size == -1
    assert _run_limit(size=Limit().size) == _ITEMS


def test_limit_negative_size_keeps_every_element() -> None:
    for size in (-2, -3, -10):
        assert _run_limit(size=size) == _ITEMS


def test_limit_positive_size_takes_the_first_elements() -> None:
    assert _run_limit(size=2) == ["alpha", "beta"]


def test_limit_size_beyond_length_keeps_every_element() -> None:
    assert _run_limit(size=len(_ITEMS) + 5) == _ITEMS


def test_limit_zero_size_produces_an_empty_result() -> None:
    assert _run_limit(size=0) == []


def test_limit_single_element_list_keeps_that_element() -> None:
    assert _run_limit(size=-1, values=["only"]) == ["only"]
