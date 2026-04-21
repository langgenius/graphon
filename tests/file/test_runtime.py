from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from graphon.file.runtime import (
    WorkflowFileRuntimeNotConfiguredError,
    WorkflowFileRuntimeRegistry,
    get_workflow_file_runtime,
    peek_workflow_file_runtime,
    use_workflow_file_runtime,
)


def test_runtime_registry_raises_until_explicitly_configured() -> None:
    registry = WorkflowFileRuntimeRegistry()

    with pytest.raises(
        WorkflowFileRuntimeNotConfiguredError,
        match="configure_workflow_file_runtime",
    ):
        registry.get()


def test_runtime_registry_peek_returns_none_when_unconfigured() -> None:
    registry = WorkflowFileRuntimeRegistry()

    assert registry.peek() is None


def test_runtime_registry_use_restores_previous_runtime() -> None:
    outer_runtime = MagicMock()
    scoped_runtime = MagicMock()
    registry = WorkflowFileRuntimeRegistry(outer_runtime)

    with registry.use(scoped_runtime) as configured_runtime:
        assert configured_runtime is scoped_runtime
        assert registry.get() is scoped_runtime

    assert registry.get() is outer_runtime


def test_peek_workflow_file_runtime_returns_current_module_runtime() -> None:
    previous_runtime = peek_workflow_file_runtime()
    scoped_runtime = MagicMock()

    with use_workflow_file_runtime(scoped_runtime):
        assert peek_workflow_file_runtime() is scoped_runtime

    assert peek_workflow_file_runtime() is previous_runtime


def test_use_workflow_file_runtime_restores_previous_module_runtime() -> None:
    outer_runtime = MagicMock()
    nested_runtime = MagicMock()

    with use_workflow_file_runtime(outer_runtime):
        assert get_workflow_file_runtime() is outer_runtime

        with use_workflow_file_runtime(nested_runtime):
            assert get_workflow_file_runtime() is nested_runtime

        assert get_workflow_file_runtime() is outer_runtime
