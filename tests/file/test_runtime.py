from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import graphon.file.runtime as runtime_module
from graphon.file.runtime import (
    WorkflowFileRuntimeNotConfiguredError,
    get_workflow_file_runtime,
    peek_workflow_file_runtime,
    set_workflow_file_runtime,
)


@pytest.fixture(autouse=True)
def _reset_file_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_module,
        "_default_workflow_file_runtime",
        None,
    )


def test_file_runtime_is_unconfigured_until_set() -> None:
    assert peek_workflow_file_runtime() is None

    with pytest.raises(
        WorkflowFileRuntimeNotConfiguredError,
        match="set_workflow_file_runtime",
    ):
        get_workflow_file_runtime()


def test_set_file_runtime_replaces_the_default() -> None:
    set_workflow_file_runtime(MagicMock())
    configured_runtime = MagicMock()

    set_workflow_file_runtime(configured_runtime)

    assert peek_workflow_file_runtime() is configured_runtime
    assert get_workflow_file_runtime() is configured_runtime


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
