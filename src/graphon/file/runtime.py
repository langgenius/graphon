from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from .protocols import WorkflowFileRuntimeProtocol


class WorkflowFileRuntimeNotConfiguredError(RuntimeError):
    """Raised when workflow file runtime dependencies were not configured."""


_default_workflow_file_runtime: WorkflowFileRuntimeProtocol | None = None
_current_workflow_file_runtime: ContextVar[WorkflowFileRuntimeProtocol | None] = (
    ContextVar("workflow_file_runtime")
)


@contextmanager
def use_workflow_file_runtime(
    runtime: WorkflowFileRuntimeProtocol | None,
) -> Iterator[None]:
    """Bind file helpers in this context; None explicitly disables resolution.

    Hosts can use this when constructing an engine or rendering its file values.
    The previous binding is restored on exit, including when an operation fails.
    """
    token = _current_workflow_file_runtime.set(runtime)
    try:
        yield
    finally:
        _current_workflow_file_runtime.reset(token)


def set_workflow_file_runtime(runtime: WorkflowFileRuntimeProtocol) -> None:
    global _default_workflow_file_runtime  # ruff:ignore[global-statement]
    _default_workflow_file_runtime = runtime


def get_workflow_file_runtime() -> WorkflowFileRuntimeProtocol:
    runtime = peek_workflow_file_runtime()
    if runtime is None:
        msg = (
            "workflow file runtime is not configured; call "
            "set_workflow_file_runtime(...) first"
        )
        raise WorkflowFileRuntimeNotConfiguredError(msg)
    return runtime


def peek_workflow_file_runtime() -> WorkflowFileRuntimeProtocol | None:
    return _current_workflow_file_runtime.get(_default_workflow_file_runtime)
