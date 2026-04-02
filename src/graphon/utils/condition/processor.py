import json
from collections.abc import Mapping, Sequence
from typing import Literal, NamedTuple

from graphon.file import file_manager
from graphon.file.enums import FileAttribute
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import (
    ArrayBooleanSegment,
    ArrayFileSegment,
    BooleanSegment,
)

from .entities import Condition, SubCondition, SupportedComparisonOperator

_FILE_SUB_CONDITION_OPERATORS = frozenset(("contains", "not contains", "all of"))
_EXISTENCE_OPERATORS = frozenset(("exists", "not exists"))


def _convert_to_bool(value: object) -> bool:
    match value:
        case int():
            result = bool(value)
        case str():
            loaded = json.loads(value)
            match loaded:
                case int() | bool():
                    result = bool(loaded)
                case _:
                    msg = f"unexpected value: type={type(value)}, value={value}"
                    raise TypeError(msg)
        case _:
            msg = f"unexpected value: type={type(value)}, value={value}"
            raise TypeError(msg)
    return result


class ConditionCheckResult(NamedTuple):
    inputs: Sequence[Mapping[str, object]]
    group_results: Sequence[bool]
    final_result: bool


class ConditionProcessor:
    def process_conditions(
        self,
        *,
        variable_pool: VariablePool,
        conditions: Sequence[Condition],
        operator: Literal["and", "or"],
    ) -> ConditionCheckResult:
        input_conditions: list[Mapping[str, object]] = []
        group_results: list[bool] = []

        for condition in conditions:
            variable = variable_pool.get(condition.variable_selector)
            if variable is None:
                msg = f"Variable {condition.variable_selector} not found"
                raise ValueError(msg)

            if _should_process_file_sub_conditions(
                variable=variable,
                comparison_operator=condition.comparison_operator,
            ):
                # check sub conditions
                if not condition.sub_variable_condition:
                    msg = "Sub variable is required"
                    raise ValueError(msg)
                result = _process_sub_conditions(
                    variable=variable,
                    sub_conditions=condition.sub_variable_condition.conditions,
                    operator=condition.sub_variable_condition.logical_operator,
                )
            elif condition.comparison_operator in _EXISTENCE_OPERATORS:
                result = _evaluate_condition(
                    value=variable.value,
                    operator=condition.comparison_operator,
                    expected=None,
                )
            else:
                actual_value = variable.value
                expected_value = _prepare_expected_value(
                    variable=variable,
                    variable_pool=variable_pool,
                    expected_value=condition.value,
                )
                input_conditions.append({
                    "actual_value": actual_value,
                    "expected_value": expected_value,
                    "comparison_operator": condition.comparison_operator,
                })
                result = _evaluate_condition(
                    value=actual_value,
                    operator=condition.comparison_operator,
                    expected=expected_value,
                )
            group_results.append(result)
            # Implemented short-circuit evaluation for logical conditions
            if (operator == "and" and not result) or (operator == "or" and result):
                final_result = result
                return ConditionCheckResult(
                    input_conditions,
                    group_results,
                    final_result,
                )

        final_result = all(group_results) if operator == "and" else any(group_results)
        return ConditionCheckResult(input_conditions, group_results, final_result)


def _evaluate_condition(
    *,
    operator: SupportedComparisonOperator,
    value: object,
    expected: str | Sequence[str] | bool | Sequence[bool] | None,
) -> bool:
    match operator:
        case "contains":
            result = _assert_contains(value=value, expected=expected)
        case "not contains":
            result = _assert_not_contains(value=value, expected=expected)
        case "start with":
            result = _assert_start_with(value=value, expected=expected)
        case "end with":
            result = _assert_end_with(value=value, expected=expected)
        case "is":
            result = _assert_is(value=value, expected=expected)
        case "is not":
            result = _assert_is_not(value=value, expected=expected)
        case "empty":
            result = _assert_empty(value=value)
        case "not empty":
            result = _assert_not_empty(value=value)
        case "=":
            result = _assert_equal(value=value, expected=expected)
        case "≠":
            result = _assert_not_equal(value=value, expected=expected)
        case ">":
            result = _assert_greater_than(value=value, expected=expected)
        case "<":
            result = _assert_less_than(value=value, expected=expected)
        case "≥":
            result = _assert_greater_than_or_equal(value=value, expected=expected)
        case "≤":
            result = _assert_less_than_or_equal(value=value, expected=expected)
        case "null":
            result = _assert_null(value=value)
        case "not null":
            result = _assert_not_null(value=value)
        case "in":
            result = _assert_in(value=value, expected=expected)
        case "not in":
            result = _assert_not_in(value=value, expected=expected)
        case "all of":
            result = _evaluate_all_of_condition(value=value, expected=expected)
        case "exists":
            result = _assert_exists(value=value)
        case "not exists":
            result = _assert_not_exists(value=value)
        case _:
            msg = f"Unsupported operator: {operator}"
            raise ValueError(msg)
    return result


def _should_process_file_sub_conditions(
    *,
    variable: object,
    comparison_operator: SupportedComparisonOperator,
) -> bool:
    match variable:
        case ArrayFileSegment():
            result = comparison_operator in _FILE_SUB_CONDITION_OPERATORS
        case _:
            result = False
    return result


def _prepare_expected_value(
    *,
    variable: object,
    variable_pool: VariablePool,
    expected_value: str | Sequence[str] | bool | Sequence[bool] | None,
) -> str | Sequence[str] | bool | list[bool] | None:
    match expected_value:
        case str():
            normalized_expected_value = variable_pool.convert_template(
                expected_value,
            ).text
        case _:
            normalized_expected_value = expected_value

    if normalized_expected_value is None:
        return None

    match variable, normalized_expected_value:
        case ((BooleanSegment() | ArrayBooleanSegment()), list()):
            result = [_convert_to_bool(item) for item in normalized_expected_value]
        case ((BooleanSegment() | ArrayBooleanSegment()), _):
            result = _convert_to_bool(normalized_expected_value)
        case _:
            result = normalized_expected_value
    return result


def _evaluate_all_of_condition(*, value: object, expected: object) -> bool:
    match expected:
        case list() if all(isinstance(item, str) for item in expected):
            str_list: list[str] = [item for item in expected if isinstance(item, str)]
            result = _assert_all_of(value=value, expected=str_list)
        case list() if all(isinstance(item, bool) for item in expected):
            bool_list: list[bool] = [
                item for item in expected if isinstance(item, bool)
            ]
            result = _assert_all_of_bool(value=value, expected=bool_list)
        case _:
            msg = "all of operator expects homogeneous list of strings or booleans"
            raise ValueError(msg)
    return result


def _assert_contains(*, value: object, expected: object) -> bool:
    if not value:
        return False

    match value:
        case str():
            match expected:
                case str():
                    normalized_expected = expected
                case _:
                    normalized_expected = str(expected)
            result = normalized_expected in value
        case list():
            result = expected in value
        case _:
            msg = "Invalid actual value type: string or array"
            raise ValueError(msg)
    return result


def _assert_not_contains(*, value: object, expected: object) -> bool:
    if not value:
        return True

    match value:
        case str():
            match expected:
                case str():
                    normalized_expected = expected
                case _:
                    normalized_expected = str(expected)
            result = normalized_expected not in value
        case list():
            result = expected not in value
        case _:
            msg = "Invalid actual value type: string or array"
            raise ValueError(msg)
    return result


def _assert_start_with(*, value: object, expected: object) -> bool:
    if not value:
        return False

    match value, expected:
        case str(), str():
            result = value.startswith(expected)
        case str(), _:
            msg = "Expected value must be a string for startswith"
            raise ValueError(msg)
        case _:
            msg = "Invalid actual value type: string"
            raise ValueError(msg)
    return result


def _assert_end_with(*, value: object, expected: object) -> bool:
    if not value:
        return False

    match value, expected:
        case str(), str():
            result = value.endswith(expected)
        case str(), _:
            msg = "Expected value must be a string for endswith"
            raise ValueError(msg)
        case _:
            msg = "Invalid actual value type: string"
            raise ValueError(msg)
    return result


def _assert_is(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    match value:
        case str() | bool():
            result = value == expected
        case _:
            msg = "Invalid actual value type: string or boolean"
            raise ValueError(msg)
    return result


def _assert_is_not(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    match value:
        case str() | bool():
            result = value != expected
        case _:
            msg = "Invalid actual value type: string or boolean"
            raise ValueError(msg)
    return result


def _assert_empty(*, value: object) -> bool:
    return not value


def _assert_not_empty(*, value: object) -> bool:
    return bool(value)


def _normalize_numeric_values(
    value: float,
    expected: object,
) -> tuple[int | float, int | float]:
    """Normalize value and expected to compatible numeric types for comparison.

    Args:
        value: The actual numeric value (int or float)
        expected: The expected value (int, float, or str)

    Returns:
        A tuple of (normalized_value, normalized_expected) with compatible types

    Raises:
        ValueError: If expected cannot be converted to a number

    """
    match expected:
        case str():
            try:
                expected_float = float(expected)
            except ValueError as e:
                msg = f"Cannot convert '{expected}' to number"
                raise ValueError(msg) from e

            match value:
                case int() if expected_float.is_integer():
                    normalized_values = (value, int(expected_float))
                case int():
                    normalized_values = (float(value), expected_float)
                case _:
                    normalized_values = (value, expected_float)
        case float():
            match value:
                case int():
                    normalized_values = (float(value), expected)
                case _:
                    normalized_values = (value, expected)
        case int():
            normalized_values = (value, expected)
        case _:
            msg = f"Cannot convert {type(expected)} to number"
            raise ValueError(msg)
    return normalized_values


def _normalize_numeric_equality_expected(*, value: object, expected: object) -> object:
    match value:
        case bool():
            match expected:
                case bool() | int() | str():
                    normalized_expected = bool(expected)
                case _:
                    msg = f"Cannot convert {type(expected)} to bool"
                    raise ValueError(msg)
        case int():
            match expected:
                case int() | float() | str():
                    normalized_expected = int(expected)
                case _:
                    msg = f"Cannot convert {type(expected)} to int"
                    raise ValueError(msg)
        case float():
            match expected:
                case int() | float() | str():
                    normalized_expected = float(expected)
                case _:
                    msg = f"Cannot convert {type(expected)} to float"
                    raise ValueError(msg)
        case _:
            msg = "Invalid actual value type: number or boolean"
            raise ValueError(msg)
    return normalized_expected


def _assert_equal(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    normalized_expected = _normalize_numeric_equality_expected(
        value=value,
        expected=expected,
    )
    return value == normalized_expected


def _assert_not_equal(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    normalized_expected = _normalize_numeric_equality_expected(
        value=value,
        expected=expected,
    )
    return value != normalized_expected


def _assert_greater_than(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    match value:
        case int() | float():
            normalized_value, normalized_expected = _normalize_numeric_values(
                value,
                expected,
            )
            result = normalized_value > normalized_expected
        case _:
            msg = "Invalid actual value type: number"
            raise ValueError(msg)
    return result


def _assert_less_than(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    match value:
        case int() | float():
            normalized_value, normalized_expected = _normalize_numeric_values(
                value,
                expected,
            )
            result = normalized_value < normalized_expected
        case _:
            msg = "Invalid actual value type: number"
            raise ValueError(msg)
    return result


def _assert_greater_than_or_equal(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    match value:
        case int() | float():
            normalized_value, normalized_expected = _normalize_numeric_values(
                value,
                expected,
            )
            result = normalized_value >= normalized_expected
        case _:
            msg = "Invalid actual value type: number"
            raise ValueError(msg)
    return result


def _assert_less_than_or_equal(*, value: object, expected: object) -> bool:
    if value is None:
        return False

    match value:
        case int() | float():
            normalized_value, normalized_expected = _normalize_numeric_values(
                value,
                expected,
            )
            result = normalized_value <= normalized_expected
        case _:
            msg = "Invalid actual value type: number"
            raise ValueError(msg)
    return result


def _assert_null(*, value: object) -> bool:
    return value is None


def _assert_not_null(*, value: object) -> bool:
    return value is not None


def _assert_in(*, value: object, expected: object) -> bool:
    if not value:
        return False

    match expected:
        case list():
            result = value in expected
        case _:
            msg = "Invalid expected value type: array"
            raise ValueError(msg)
    return result


def _assert_not_in(*, value: object, expected: object) -> bool:
    if not value:
        return True

    match expected:
        case list():
            result = value not in expected
        case _:
            msg = "Invalid expected value type: array"
            raise ValueError(msg)
    return result


def _assert_all_of(*, value: object, expected: Sequence[str]) -> bool:
    if not value:
        return False

    match value:
        case list() | tuple() | set() | str():
            result = all(item in value for item in expected)
        case _:
            result = False
    return result


def _assert_all_of_bool(*, value: object, expected: Sequence[bool]) -> bool:
    if not value:
        return False

    match value:
        case list() | tuple() | set():
            result = all(item in value for item in expected)
        case _:
            result = False
    return result


def _assert_exists(*, value: object) -> bool:
    return value is not None


def _assert_not_exists(*, value: object) -> bool:
    return value is None


def _process_sub_conditions(
    variable: ArrayFileSegment,
    sub_conditions: Sequence[SubCondition],
    operator: Literal["and", "or"],
) -> bool:
    files = variable.value
    group_results: list[bool] = []
    for condition in sub_conditions:
        key = FileAttribute(condition.key)
        values = [file_manager.get_attr(file=file, attr=key) for file in files]
        expected_value = condition.value
        if key == FileAttribute.EXTENSION:
            if not isinstance(expected_value, str):
                msg = (
                    "Expected value must be a string when key is "
                    "FileAttribute.EXTENSION"
                )
                raise TypeError(msg)
            if expected_value and not expected_value.startswith("."):
                expected_value = "." + expected_value

            normalized_values: list[object] = []
            for value in values:
                if value and isinstance(value, str) and not value.startswith("."):
                    normalized_value = "." + value
                else:
                    normalized_value = value
                normalized_values.append(normalized_value)
            values = normalized_values
        sub_group_results: list[bool] = [
            _evaluate_condition(
                value=value,
                operator=condition.comparison_operator,
                expected=expected_value,
            )
            for value in values
        ]
        # Determine the result based on the presence of "not" in the comparison operator
        result = (
            all(sub_group_results)
            if "not" in condition.comparison_operator
            else any(sub_group_results)
        )
        group_results.append(result)
    return all(group_results) if operator == "and" else any(group_results)
