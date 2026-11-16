"""Scoped file markers that retain Pydantic's selected fields during projection."""

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Final

_FILE_MARKERS_ENABLED: Final[ContextVar[bool]] = ContextVar(
    "file_markers_enabled", default=False
)


class SerializedFile:  # ruff: ignore[class-as-data-structure] - Pydantic would serialize a dataclass and lose the file marker.
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


def file_markers_enabled() -> bool:
    return _FILE_MARKERS_ENABLED.get()


@contextmanager
def mark_serialized_files() -> Generator[None]:
    """Mark files while Pydantic projects their enclosing models."""
    token = _FILE_MARKERS_ENABLED.set(True)
    try:
        yield
    finally:
        _FILE_MARKERS_ENABLED.reset(token)
