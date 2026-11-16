from typing import Literal

import pytest

from graphon.runtime import ReadOnlyVariablePoolWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.utils.condition.entities import Condition
from graphon.utils.condition.processor import ConditionProcessor


def test_number_formatting() -> None:
    condition_processor = ConditionProcessor()
    variable_pool = VariablePool()
    variable_pool.add(["test_node_id", "zone"], 0)
    variable_pool.add(["test_node_id", "one"], 1)
    variable_pool.add(["test_node_id", "one_one"], 1.1)

    assert (
        condition_processor.process_conditions(
            variable_pool=variable_pool,
            conditions=[
                Condition(
                    variable_selector=["test_node_id", "zone"],
                    comparison_operator="≤",
                    value="0.95",
                ),
            ],
            operator="or",
        ).final_result
        is True
    )

    assert (
        condition_processor.process_conditions(
            variable_pool=variable_pool,
            conditions=[
                Condition(
                    variable_selector=["test_node_id", "one"],
                    comparison_operator="≥",
                    value="0.95",
                ),
            ],
            operator="or",
        ).final_result
        is True
    )

    assert (
        condition_processor.process_conditions(
            variable_pool=variable_pool,
            conditions=[
                Condition(
                    variable_selector=["test_node_id", "one_one"],
                    comparison_operator="≥",
                    value="0.95",
                ),
            ],
            operator="or",
        ).final_result
        is True
    )

    assert (
        condition_processor.process_conditions(
            variable_pool=variable_pool,
            conditions=[
                Condition(
                    variable_selector=["test_node_id", "one_one"],
                    comparison_operator=">",
                    value="0",
                ),
            ],
            operator="or",
        ).final_result
        is True
    )


def test_process_conditions_converts_boolean_expected_from_string() -> None:
    condition_processor = ConditionProcessor()
    variable_pool = VariablePool()
    variable_pool.add(["test_node_id", "enabled"], False)

    result = condition_processor.process_conditions(
        variable_pool=variable_pool,
        conditions=[
            Condition(
                variable_selector=["test_node_id", "enabled"],
                comparison_operator="is",
                value="false",
            ),
        ],
        operator="and",
    )

    assert result.final_result is True


def test_process_conditions_contains_supports_string_and_list_values() -> None:
    condition_processor = ConditionProcessor()
    variable_pool = VariablePool()
    variable_pool.add(["test_node_id", "text"], "graphon")
    variable_pool.add(["test_node_id", "tags"], ["a", "b"])

    text_result = condition_processor.process_conditions(
        variable_pool=variable_pool,
        conditions=[
            Condition(
                variable_selector=["test_node_id", "text"],
                comparison_operator="contains",
                value="pho",
            ),
        ],
        operator="and",
    )
    list_result = condition_processor.process_conditions(
        variable_pool=variable_pool,
        conditions=[
            Condition(
                variable_selector=["test_node_id", "tags"],
                comparison_operator="contains",
                value="a",
            ),
        ],
        operator="and",
    )

    assert text_result.final_result is True
    assert list_result.final_result is True


@pytest.mark.parametrize("comparison_operator", ["in", "not in"])
@pytest.mark.parametrize(
    ("actual_value", "members", "matches"),
    [
        ("", ["", "a"], True),
        ("", ["a"], False),
        ("", [], False),
        (False, ["false", "true"], True),
        (False, ["true"], False),
        (False, [], False),
        ("a", ["a"], True),
        ("b", ["a"], False),
        (True, ["true"], True),
        (None, [""], False),
    ],
)
def test_process_conditions_membership(
    actual_value: str | bool | None,
    members: list[str],
    matches: bool,
    comparison_operator: Literal["in", "not in"],
) -> None:
    variable_pool = VariablePool()
    variable_pool.add(["start", "choice"], actual_value)

    result = ConditionProcessor().process_conditions(
        variable_pool=variable_pool,
        conditions=[
            Condition(
                variable_selector=["start", "choice"],
                comparison_operator=comparison_operator,
                value=members,
            ),
        ],
        operator="and",
    )

    assert result.final_result is (
        matches if comparison_operator == "in" else not matches
    )


@pytest.mark.parametrize("comparison_operator", ["in", "not in"])
@pytest.mark.parametrize("actual_value", ["", False])
def test_process_conditions_membership_requires_list(
    actual_value: str | bool,
    comparison_operator: Literal["in", "not in"],
) -> None:
    variable_pool = VariablePool()
    variable_pool.add(["start", "choice"], actual_value)

    with pytest.raises(ValueError, match="Invalid expected value type: array"):
        ConditionProcessor().process_conditions(
            variable_pool=variable_pool,
            conditions=[
                Condition(
                    variable_selector=["start", "choice"],
                    comparison_operator=comparison_operator,
                    value="true",
                ),
            ],
            operator="and",
        )


def test_process_conditions_resolves_templates_from_read_only_variable_pool() -> None:
    condition_processor = ConditionProcessor()
    variable_pool = VariablePool()
    variable_pool.add(["test_node_id", "text"], "graphon")
    variable_pool.add(["test_node_id", "needle"], "pho")

    result = condition_processor.process_conditions(
        variable_pool=ReadOnlyVariablePoolWrapper(variable_pool),
        conditions=[
            Condition(
                variable_selector=["test_node_id", "text"],
                comparison_operator="contains",
                value="{{#test_node_id.needle#}}",
            ),
        ],
        operator="and",
    )

    assert result.final_result is True
