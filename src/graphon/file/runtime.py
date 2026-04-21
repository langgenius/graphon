from __future__ import annotations

from .protocols import WorkflowFileRuntimeProtocol


class WorkflowFileRuntimeNotConfiguredError(RuntimeError):
    """Raised when workflow file runtime dependencies were not configured."""


def _not_configured_error() -> WorkflowFileRuntimeNotConfiguredError:
    msg = (
        "workflow file runtime is not configured; call "
        "set_workflow_file_runtime(...) first"
    )
    return WorkflowFileRuntimeNotConfiguredError(msg)


class WorkflowFileRuntimeRegistry:
    """Small helper that keeps runtime configuration explicit."""

    def __init__(
        self,
        runtime: WorkflowFileRuntimeProtocol | None = None,
    ) -> None:
        self._runtime = runtime

    def set(
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


_workflow_file_runtime_registry = WorkflowFileRuntimeRegistry()


def configure_workflow_file_runtime(
    runtime: WorkflowFileRuntimeProtocol,
) -> WorkflowFileRuntimeProtocol:
    """Compatibility alias for set_workflow_file_runtime()."""
    return _workflow_file_runtime_registry.set(runtime)


def set_workflow_file_runtime(runtime: WorkflowFileRuntimeProtocol) -> None:
    _workflow_file_runtime_registry.set(runtime)


def get_workflow_file_runtime() -> WorkflowFileRuntimeProtocol:
    return _workflow_file_runtime_registry.get()


def peek_workflow_file_runtime() -> WorkflowFileRuntimeProtocol | None:
    return _workflow_file_runtime_registry.peek()
