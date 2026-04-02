from typing import Any

from graphon.variables.types import SegmentType

from .enums import Operation

_NO_VALUE_OPERATIONS = frozenset((
    Operation.CLEAR,
    Operation.REMOVE_FIRST,
    Operation.REMOVE_LAST,
))
_LIST_VALUE_OPERATIONS = frozenset((Operation.EXTEND, Operation.OVER_WRITE))


def _is_valid_list_input(
    value: Any,
    item_type: type[Any] | tuple[type[Any], ...],
) -> bool:
    match value:
        case list() if all(isinstance(item, item_type) for item in value):
            result = True
        case _:
            result = False
    return result


def _is_valid_numeric_input(*, operation: Operation, value: Any) -> bool:
    match value:
        case int() | float():
            result = not (operation == Operation.DIVIDE and value == 0)
        case _:
            result = False
    return result


def is_operation_supported(*, variable_type: SegmentType, operation: Operation):
    match operation:
        case Operation.OVER_WRITE | Operation.CLEAR:
            return True
        case Operation.SET:
            return variable_type in frozenset((
                SegmentType.OBJECT,
                SegmentType.STRING,
                SegmentType.NUMBER,
                SegmentType.INTEGER,
                SegmentType.FLOAT,
                SegmentType.BOOLEAN,
            ))
        case Operation.ADD | Operation.SUBTRACT | Operation.MULTIPLY | Operation.DIVIDE:
            # Only number variable can be added, subtracted, multiplied or divided
            return variable_type in frozenset((
                SegmentType.NUMBER,
                SegmentType.INTEGER,
                SegmentType.FLOAT,
            ))
        case (
            Operation.APPEND
            | Operation.EXTEND
            | Operation.REMOVE_FIRST
            | Operation.REMOVE_LAST
        ):
            # Only array variable can be appended or extended
            # Only array variable can have elements removed
            return variable_type.is_array_type()


def is_variable_input_supported(*, operation: Operation):
    return operation not in frozenset((
        Operation.SET,
        Operation.ADD,
        Operation.SUBTRACT,
        Operation.MULTIPLY,
        Operation.DIVIDE,
    ))


def is_constant_input_supported(*, variable_type: SegmentType, operation: Operation):
    match variable_type:
        case SegmentType.STRING | SegmentType.OBJECT | SegmentType.BOOLEAN:
            return operation in frozenset((Operation.OVER_WRITE, Operation.SET))
        case SegmentType.NUMBER | SegmentType.INTEGER | SegmentType.FLOAT:
            return operation in frozenset((
                Operation.OVER_WRITE,
                Operation.SET,
                Operation.ADD,
                Operation.SUBTRACT,
                Operation.MULTIPLY,
                Operation.DIVIDE,
            ))
        case _:
            return False


def is_input_value_valid(
    *,
    variable_type: SegmentType,
    operation: Operation,
    value: Any,
):
    if operation in _NO_VALUE_OPERATIONS:
        result = True
    else:
        match variable_type, operation, value:
            case SegmentType.STRING, _, str():
                result = True
            case SegmentType.BOOLEAN, _, bool():
                result = True
            case (
                SegmentType.NUMBER | SegmentType.INTEGER | SegmentType.FLOAT,
                _,
                _,
            ):
                result = _is_valid_numeric_input(operation=operation, value=value)
            case SegmentType.OBJECT, _, dict():
                result = True
            case (
                SegmentType.ARRAY_ANY,
                Operation.APPEND,
                str() | float() | int() | dict(),
            ):
                result = True
            case SegmentType.ARRAY_STRING, Operation.APPEND, str():
                result = True
            case SegmentType.ARRAY_NUMBER, Operation.APPEND, int() | float():
                result = True
            case SegmentType.ARRAY_OBJECT, Operation.APPEND, dict():
                result = True
            case SegmentType.ARRAY_BOOLEAN, Operation.APPEND, bool():
                result = True
            case SegmentType.ARRAY_ANY, operation, _ if (
                operation in _LIST_VALUE_OPERATIONS
            ):
                result = _is_valid_list_input(value, (str, float, int, dict))
            case SegmentType.ARRAY_STRING, operation, _ if (
                operation in _LIST_VALUE_OPERATIONS
            ):
                result = _is_valid_list_input(value, str)
            case SegmentType.ARRAY_NUMBER, operation, _ if (
                operation in _LIST_VALUE_OPERATIONS
            ):
                result = _is_valid_list_input(value, (int, float))
            case SegmentType.ARRAY_OBJECT, operation, _ if (
                operation in _LIST_VALUE_OPERATIONS
            ):
                result = _is_valid_list_input(value, dict)
            case (
                SegmentType.ARRAY_BOOLEAN,
                operation,
                _,
            ) if operation in _LIST_VALUE_OPERATIONS:
                result = _is_valid_list_input(value, bool)
            case _:
                result = False
    return result
