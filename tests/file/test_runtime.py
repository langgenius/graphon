from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import graphon.file.runtime as runtime_module
from graphon.file.runtime import (
    WorkflowFileRuntimeNotConfiguredError,
    WorkflowFileRuntimeRegistry,
    get_workflow_file_runtime,
    peek_workflow_file_runtime,
    set_workflow_file_runtime,
)

_pytestmark = pytest.mark.usefixtures("_reset_workflow_file_runtime_registry")


@pytest.fixture
def _reset_workflow_file_runtime_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_module,
        "_workflow_file_runtime_registry",
        WorkflowFileRuntimeRegistry(),
    )


def test_runtime_registry_raises_until_explicitly_configured() -> None:
    registry = WorkflowFileRuntimeRegistry()

    with pytest.raises(
        WorkflowFileRuntimeNotConfiguredError,
        match="set_workflow_file_runtime",
    ):
        registry.get()


def test_runtime_registry_peek_returns_none_when_unconfigured() -> None:
    registry = WorkflowFileRuntimeRegistry()

    assert registry.peek() is None


def test_runtime_registry_set_updates_current_runtime() -> None:
    configured_runtime = MagicMock()
    registry = WorkflowFileRuntimeRegistry()

    assert registry.set(configured_runtime) is configured_runtime
    assert registry.get() is configured_runtime


def test_peek_workflow_file_runtime_returns_current_module_runtime() -> None:
    configured_runtime = MagicMock()

    set_workflow_file_runtime(configured_runtime)

    assert peek_workflow_file_runtime() is configured_runtime


def test_set_workflow_file_runtime_updates_module_runtime() -> None:
    configured_runtime = MagicMock()

    set_workflow_file_runtime(configured_runtime)

    assert get_workflow_file_runtime() is configured_runtime


@pytest.mark.usefixtures("_reset_workflow_file_runtime_registry")
def test_file_runtime_scope_restores_nested_and_unconfigured_callers() -> None:
    outer = MagicMock()
    inner = MagicMock()
    default = MagicMock()

    def fail_in_inner_scope() -> None:
        with runtime_module.use_workflow_file_runtime(inner):
            assert get_workflow_file_runtime() is inner
            message = "file read failed"
            raise RuntimeError(message)

    with runtime_module.use_workflow_file_runtime(outer):
        assert get_workflow_file_runtime() is outer
        set_workflow_file_runtime(default)
        with pytest.raises(RuntimeError, match="file read failed"):
            fail_in_inner_scope()
        assert get_workflow_file_runtime() is outer
        with runtime_module.use_workflow_file_runtime(None):
            assert peek_workflow_file_runtime() is None
            with pytest.raises(WorkflowFileRuntimeNotConfiguredError):
                get_workflow_file_runtime()
        assert get_workflow_file_runtime() is outer
    assert get_workflow_file_runtime() is default
