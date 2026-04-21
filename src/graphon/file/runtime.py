from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from .protocols import WorkflowFileRuntimeProtocol


class WorkflowFileRuntimeNotConfiguredError(RuntimeError):
    """Raised when workflow file runtime dependencies were not configured."""


def _not_configured_error() -> WorkflowFileRuntimeNotConfiguredError:
    msg = (
        "workflow file runtime is not configured; call "
        "configure_workflow_file_runtime(...) first or use "
        "use_workflow_file_runtime(...) for a scoped override"
    )
    return WorkflowFileRuntimeNotConfiguredError(msg)


class WorkflowFileRuntimeRegistry:
    """Small helper that keeps runtime configuration explicit and scoped."""

    def __init__(
        self,
        runtime: WorkflowFileRuntimeProtocol | None = None,
    ) -> None:
        self._runtime = runtime

    def configure(
        self,
        runtime: WorkflowFileRuntimeProtocol,
    ) -> WorkflowFileRuntimeProtocol:
        self._runtime = runtime
        return runtime

    def peek(self) -> WorkflowFileRuntimeProtocol | None:
        return self._runtime

    def get(self) -> WorkflowFileRuntimeProtocol:
        runtime = self.peek()
        if runtime is None:
            raise _not_configured_error()
        return runtime

    @contextmanager
    def use(
        self,
        runtime: WorkflowFileRuntimeProtocol,
    ) -> Iterator[WorkflowFileRuntimeProtocol]:
        previous_runtime = self.peek()
        self.configure(runtime)
        try:
            yield runtime
        finally:
            self._runtime = previous_runtime


workflow_file_runtime_registry = WorkflowFileRuntimeRegistry()


def configure_workflow_file_runtime(
    runtime: WorkflowFileRuntimeProtocol,
) -> WorkflowFileRuntimeProtocol:
    """Configure the default runtime used by file helpers."""
    return workflow_file_runtime_registry.configure(runtime)


@contextmanager
def use_workflow_file_runtime(
    runtime: WorkflowFileRuntimeProtocol,
) -> Iterator[WorkflowFileRuntimeProtocol]:
    """Temporarily override the configured runtime within a scope."""
    with workflow_file_runtime_registry.use(runtime) as configured_runtime:
        yield configured_runtime


def set_workflow_file_runtime(runtime: WorkflowFileRuntimeProtocol) -> None:
    """Backward-compatible alias for configure_workflow_file_runtime()."""
    configure_workflow_file_runtime(runtime)


def get_workflow_file_runtime() -> WorkflowFileRuntimeProtocol:
    return workflow_file_runtime_registry.get()


def peek_workflow_file_runtime() -> WorkflowFileRuntimeProtocol | None:
    return workflow_file_runtime_registry.peek()
