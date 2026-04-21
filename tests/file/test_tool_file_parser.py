from collections.abc import Iterator

import pytest

from graphon.file.tool_file_parser import (
    ToolFileManagerFactoryNotSetError,
    get_tool_file_manager_factory,
    require_tool_file_manager_factory,
    set_tool_file_manager_factory,
    tool_file_manager_factory_registry,
    use_tool_file_manager_factory,
)


@pytest.fixture(autouse=True)
def _reset_tool_file_manager_factory_registry() -> Iterator[None]:
    tool_file_manager_factory_registry.clear()
    yield
    tool_file_manager_factory_registry.clear()


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


def test_use_tool_file_manager_factory_scopes_and_restores_factory() -> None:
    def base_factory() -> str:
        return "base"

    def scoped_factory() -> str:
        return "scoped"

    set_tool_file_manager_factory(base_factory)

    with use_tool_file_manager_factory(scoped_factory) as active_factory:
        assert active_factory is scoped_factory
        assert require_tool_file_manager_factory()() == "scoped"

    assert require_tool_file_manager_factory()() == "base"


def test_nested_use_tool_file_manager_factory_restores_previous_scope() -> None:
    def outer_factory() -> str:
        return "outer"

    def inner_factory() -> str:
        return "inner"

    with use_tool_file_manager_factory(outer_factory):
        assert require_tool_file_manager_factory()() == "outer"

        with use_tool_file_manager_factory(inner_factory):
            assert require_tool_file_manager_factory()() == "inner"

        assert require_tool_file_manager_factory()() == "outer"

    assert get_tool_file_manager_factory() is None
