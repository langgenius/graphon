from collections.abc import Callable
from typing import Any

import pytest

import graphon.file.tool_file_parser as tool_file_parser_module
from graphon.file.tool_file_parser import (
    ToolFileManagerFactoryNotSetError,
    get_tool_file_manager_factory,
    require_tool_file_manager_factory,
    set_tool_file_manager_factory,
)


class _RegistryHarness:
    def __init__(self) -> None:
        self._factory: Callable[[], Any] | None = None

    def get(self) -> Callable[[], Any] | None:
        return self._factory

    def require(self) -> Callable[[], Any]:
        if self._factory is None:
            raise ToolFileManagerFactoryNotSetError
        return self._factory

    def set(self, factory: Callable[[], Any]) -> None:
        self._factory = factory


@pytest.fixture(autouse=True)
def _reset_tool_file_manager_factory_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        tool_file_parser_module,
        "_tool_file_manager_factory_registry",
        _RegistryHarness(),
    )


def test_get_returns_none_when_no_factory_is_configured() -> None:
    assert get_tool_file_manager_factory() is None


def test_require_raises_clear_error_when_factory_is_missing() -> None:
    with pytest.raises(ToolFileManagerFactoryNotSetError, match="not configured"):
        require_tool_file_manager_factory()


def test_set_tool_file_manager_factory_preserves_compatibility() -> None:
    def factory() -> str:
        return "configured"

    set_tool_file_manager_factory(factory)

    assert get_tool_file_manager_factory() is factory
    assert require_tool_file_manager_factory()() == "configured"
