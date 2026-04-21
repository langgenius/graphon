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


@pytest.fixture(autouse=True)
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
