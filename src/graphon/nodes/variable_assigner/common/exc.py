from collections.abc import Sequence


class VariableOperatorNodeError(ValueError):
    """Base error type, don't use directly."""


class ReadOnlyVariableError(VariableOperatorNodeError):
    def __init__(self, *, variable_selector: Sequence[str]) -> None:
        super().__init__(f"Variable {list(variable_selector)} is read-only")
