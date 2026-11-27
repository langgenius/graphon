from collections.abc import Sequence
from typing import NamedTuple, Self


class VariableAddress(NamedTuple):
    """The scope and name of one stored variable."""

    scope: str
    name: str

    @classmethod
    def from_selector(cls, selector: Sequence[str], /) -> Self:
        match selector:
            case [str(scope), str(name)]:
                return cls(scope, name)
            case _:
                msg = "A variable address requires two strings: scope and name."
                raise ValueError(msg)
