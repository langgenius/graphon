from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from graphon.file import runtime as file_runtime
from graphon.file.runtime import (
    WorkflowFileRuntimeNotConfiguredError,
    get_workflow_file_runtime,
    peek_workflow_file_runtime,
    use_workflow_file_runtime,
)


def test_file_runtime_has_no_process_setter() -> None:
    assert not hasattr(file_runtime, "set_workflow_file_runtime")


def test_file_runtime_requires_an_explicit_scope() -> None:
    assert peek_workflow_file_runtime() is None

    with pytest.raises(
        WorkflowFileRuntimeNotConfiguredError,
        match="use_workflow_file_runtime",
    ):
        get_workflow_file_runtime()


def test_file_runtime_scope_restores_nested_and_unconfigured_callers() -> None:
    outer = MagicMock()
    inner = MagicMock()

    def fail_in_inner_scope() -> None:
        with use_workflow_file_runtime(inner):
            assert get_workflow_file_runtime() is inner
            message = "file read failed"
            raise RuntimeError(message)

    with use_workflow_file_runtime(outer):
        assert get_workflow_file_runtime() is outer
        with pytest.raises(RuntimeError, match="file read failed"):
            fail_in_inner_scope()
        assert get_workflow_file_runtime() is outer
        with use_workflow_file_runtime(None):
            assert peek_workflow_file_runtime() is None
            with pytest.raises(WorkflowFileRuntimeNotConfiguredError):
                get_workflow_file_runtime()
        assert get_workflow_file_runtime() is outer
    assert peek_workflow_file_runtime() is None
