from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

ToolFileManagerFactory = Callable[[], Any]


class ToolFileManagerFactoryNotSetError(RuntimeError):
    """Raised when code requires a configured tool file manager factory."""

    def __init__(self) -> None:
        super().__init__(
            "Tool file manager factory is not configured. "
            "Call set_tool_file_manager_factory(...) or use "
            "use_tool_file_manager_factory(...)."
        )


class ToolFileManagerFactoryRegistry:
    """Store the active tool file manager factory behind an explicit API."""

    def __init__(self) -> None:
        self._factory: ToolFileManagerFactory | None = None

    def get(self) -> ToolFileManagerFactory | None:
        return self._factory

    def require(self) -> ToolFileManagerFactory:
        factory = self.get()
        if factory is None:
            raise ToolFileManagerFactoryNotSetError
        return factory

    def set(self, factory: ToolFileManagerFactory) -> None:
        self._factory = factory

    def clear(self) -> None:
        self._factory = None

    @contextmanager
    def use(self, factory: ToolFileManagerFactory) -> Iterator[ToolFileManagerFactory]:
        previous_factory = self._factory
        self._factory = factory
        try:
            yield factory
        finally:
            self._factory = previous_factory


tool_file_manager_factory_registry = ToolFileManagerFactoryRegistry()


def get_tool_file_manager_factory() -> ToolFileManagerFactory | None:
    return tool_file_manager_factory_registry.get()


def require_tool_file_manager_factory() -> ToolFileManagerFactory:
    return tool_file_manager_factory_registry.require()


@contextmanager
def use_tool_file_manager_factory(
    factory: ToolFileManagerFactory,
) -> Iterator[ToolFileManagerFactory]:
    with tool_file_manager_factory_registry.use(factory) as active_factory:
        yield active_factory


def set_tool_file_manager_factory(factory: ToolFileManagerFactory) -> None:
    """Compatibility wrapper around the registry's explicit setter."""

    tool_file_manager_factory_registry.set(factory)


__all__ = [
    "ToolFileManagerFactory",
    "ToolFileManagerFactoryNotSetError",
    "ToolFileManagerFactoryRegistry",
    "get_tool_file_manager_factory",
    "require_tool_file_manager_factory",
    "set_tool_file_manager_factory",
    "tool_file_manager_factory_registry",
    "use_tool_file_manager_factory",
]
