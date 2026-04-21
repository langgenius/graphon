from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any

import pytest

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.graph_events.node import NodeRunSucceededEvent
from graphon.node_events.base import NodeRunResult
from graphon.nodes.if_else.if_else_node import IfElseNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params, build_variable_pool


def _condition(
    *,
    selector: tuple[str, ...] = ("start", "enabled"),
    value: str = "true",
) -> dict[str, Any]:
    return {
        "comparison_operator": "is",
        "variable_selector": list(selector),
        "value": value,
    }


def _case(
    case_id: str,
    *,
    selector: tuple[str, ...] = ("start", "enabled"),
    value: str = "true",
    logical_operator: str = "and",
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "logical_operator": logical_operator,
        "conditions": [_condition(selector=selector, value=value)],
    }


def _expected_input(
    *,
    actual_value: bool,
    expected_value: bool,
) -> dict[str, Any]:
    return {
        "actual_value": actual_value,
        "expected_value": expected_value,
        "comparison_operator": "is",
    }


def _expected_case_group(
    case_id: str,
    *,
    selector: tuple[str, ...] = ("start", "enabled"),
    value: str = "true",
    logical_operator: str = "and",
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "logical_operator": logical_operator,
        "conditions": [
            {
                "variable_selector": list(selector),
                "comparison_operator": "is",
                "value": value,
                "sub_variable_condition": None,
            }
        ],
    }


def _expected_condition_result(
    *,
    group: str | dict[str, Any],
    result: bool,
) -> dict[str, Any]:
    return {
        "group": group,
        "results": [result],
        "final_result": result,
    }


@dataclass(frozen=True)
class RunScenario:
    name: str
    data: dict[str, Any]
    expected_outputs: dict[str, Any]
    expected_edge_source_handle: str
    expected_inputs: list[dict[str, Any]]
    expected_condition_results: list[dict[str, Any]]
    variables: tuple[tuple[tuple[str, ...], Any], ...] = field(
        default_factory=lambda: ((("start", "enabled"), True),),
    )


def _run_scenarios() -> list[RunScenario]:
    return [
        RunScenario(
            name="legacy_match",
            data={
                "type": "if-else",
                "title": "If Else",
                "logical_operator": "and",
                "conditions": [_condition(value="true")],
            },
            expected_outputs={"result": True, "selected_case_id": "true"},
            expected_edge_source_handle="true",
            expected_inputs=[_expected_input(actual_value=True, expected_value=True)],
            expected_condition_results=[
                _expected_condition_result(group="default", result=True)
            ],
        ),
        RunScenario(
            name="legacy_miss_does_not_duplicate_results",
            data={
                "type": "if-else",
                "title": "If Else",
                "logical_operator": "and",
                "conditions": [_condition(value="false")],
            },
            expected_outputs={"result": False, "selected_case_id": "false"},
            expected_edge_source_handle="false",
            expected_inputs=[_expected_input(actual_value=True, expected_value=False)],
            expected_condition_results=[
                _expected_condition_result(group="default", result=False)
            ],
        ),
        RunScenario(
            name="cases_match_after_initial_miss_short_circuits",
            data={
                "type": "if-else",
                "title": "If Else",
                "logical_operator": "and",
                "conditions": [_condition(value="false")],
                "cases": [
                    _case("first", value="false"),
                    _case("second", value="true"),
                    _case("unreachable", selector=("missing", "flag"), value="true"),
                ],
            },
            expected_outputs={"result": True, "selected_case_id": "second"},
            expected_edge_source_handle="second",
            expected_inputs=[_expected_input(actual_value=True, expected_value=True)],
            expected_condition_results=[
                _expected_condition_result(
                    group=_expected_case_group("first", value="false"),
                    result=False,
                ),
                _expected_condition_result(
                    group=_expected_case_group("second", value="true"),
                    result=True,
                ),
            ],
        ),
        RunScenario(
            name="cases_miss_ignores_legacy_conditions",
            data={
                "type": "if-else",
                "title": "If Else",
                "logical_operator": "and",
                "conditions": [_condition(value="true")],
                "cases": [_case("true", value="false")],
            },
            expected_outputs={"result": False, "selected_case_id": "false"},
            expected_edge_source_handle="false",
            expected_inputs=[_expected_input(actual_value=True, expected_value=False)],
            expected_condition_results=[
                _expected_condition_result(
                    group=_expected_case_group("true", value="false"),
                    result=False,
                )
            ],
        ),
        RunScenario(
            name="explicit_empty_cases_do_not_fallback_to_legacy_conditions",
            data={
                "type": "if-else",
                "title": "If Else",
                "logical_operator": "and",
                "conditions": [_condition(value="true")],
                "cases": [],
            },
            expected_outputs={"result": False, "selected_case_id": "false"},
            expected_edge_source_handle="false",
            expected_inputs=[],
            expected_condition_results=[],
        ),
    ]


RUN_SCENARIOS = _run_scenarios()


def _run_if_else_node(
    *,
    data: dict[str, Any],
    variables: tuple[tuple[tuple[str, ...], Any], ...],
) -> tuple[NodeRunResult, list[warnings.WarningMessage]]:
    runtime_state = GraphRuntimeState(
        variable_pool=build_variable_pool(variables=variables),
        start_at=perf_counter(),
    )
    node = IfElseNode(
        node_id="if-node",
        data=IfElseNode.validate_node_data(data),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=runtime_state,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        events = list(node.run())

    result_event = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )
    return result_event.node_run_result, caught


def _assert_no_deprecation_warnings(
    caught: list[warnings.WarningMessage],
) -> None:
    assert not any(isinstance(w.message, DeprecationWarning) for w in caught)


class TestIfElseNodeRun:
    @pytest.mark.parametrize(
        "scenario",
        RUN_SCENARIOS,
        ids=[scenario.name for scenario in RUN_SCENARIOS],
    )
    def test_run_scenarios(self, scenario: RunScenario) -> None:
        result, caught = _run_if_else_node(
            data=scenario.data,
            variables=scenario.variables,
        )

        assert result.status == WorkflowNodeExecutionStatus.SUCCEEDED
        assert result.outputs == scenario.expected_outputs
        assert result.edge_source_handle == scenario.expected_edge_source_handle
        assert result.inputs == {"conditions": scenario.expected_inputs}
        assert (
            result.process_data["condition_results"]
            == scenario.expected_condition_results
        )
        _assert_no_deprecation_warnings(caught)
